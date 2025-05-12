import tempfile
import pyarrow.parquet as pq
from xml_aws_athena.parser import ParseXml
from xml_aws_athena.schema import schema_nota
import pyarrow as pa
from itertools import islice
import logging
import xml_aws_athena.write as Write
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import os
from functools import partial


logger = logging.getLogger(__name__)


def write_parquet_file(data: tuple, tmpdirname: str) -> tuple:
    """
    Write XML data to a temporary Parquet file.
    Args:
        data (tuple): Tuple containing position and data.
    Returns:
        tuple: Tuple containing position and file path.
    """
    pos, (xml, __, instatus, __, controle) = data
    file = ParseXml(controle, instatus, xml)
    file_path = f"{tmpdirname}/{pos:02d}.parquet"
    pq.write_table(file.arrow(), file_path)
    return pos, file_path


def write_parquet_temp(rst: list[tuple]) -> pa.Table:
    logger.info("Escrevendo arquivos temporários em parquet...")
    with tempfile.TemporaryDirectory(prefix="xml_mssql") as tmpdirname:
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            rst = executor.map(partial(write_parquet_file, tmpdirname=tmpdirname), rst)

        tbl_full = pq.ParquetDataset(tmpdirname, schema=schema_nota)
        tbl_full = tbl_full.read(use_threads=True)

    return tbl_full


def command_silver(start: datetime, end: datetime, rst: list[tuple]):
    logger.info(f"Total de {len(rst)} registros para processar")

    iterar = iter(rst)
    merge = False
    total = 0

    while lotes := list(islice(iterar, 1_000)):
        tbl_full = write_parquet_temp(lotes)
        total += len(lotes)

        if not Write.is_delta_table():
            logger.info(f"Criando tabela Delta Lake {total} registros")
            Write.write_deltalake_aws(tbl_full, schema_nota)
        else:
            merge = True
            logger.info(f"Atualizando tabela Delta Lake {total} registros")
            Write.merge_deltalake_aws(tbl_full)

    if merge:
        logger.info("Otimizando tabela Delta Lake")
        Write.delta_optimize_aws()

    return Write.read_deltalake_aws(start, end)
