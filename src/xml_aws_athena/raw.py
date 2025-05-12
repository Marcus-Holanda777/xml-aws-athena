from xml_aws_athena.cloud import Storage
from xml_aws_athena.connect import iter_notes
from xml_aws_athena.parser import FileXml
import os
from datetime import datetime
import logging
from itertools import islice
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from xml_aws_athena import config

logger = logging.getLogger(__name__)
CAMADA_RAW = "raw"
BUCKET_NAME = config.get("bucket_xml")


def upload_file(client: Storage, notas: tuple, put_object: bool = True) -> tuple:
    """
    Upload XML file to S3 bucket.
    Args:
        client (Storage): Storage client.
        notas (tuple): Tuple containing position and data.
        put_object (bool, optional): Flag to upload the object. Defaults to True.
    Returns:
        str: S3 path where the file was uploaded.
    """
    pos, data = notas
    file = FileXml(*data)
    file_to, file_xml = file.export_file_xml()

    file_raw_to = f"{CAMADA_RAW}/{file_to}"

    if put_object:
        client.put_object_file(file_xml, BUCKET_NAME, file_raw_to)

    logger.info(f"Nota - {pos:02d} - {file_raw_to}, s3: {put_object}")

    return notas


def comand_raw(
    server: str,
    database: str,
    tips: list[str],
    start: datetime,
    end: datetime,
    limit: int = None,
    put_object: bool = True,
    font: str = "dbnfe",
) -> list[tuple]:
    """
    Upload XML files to S3 bucket.
    Args:
        server (str): Server name.
        database (str): Database name.
        tips (list[str]): List of tips.
        start (datetime): Start date.
        end (datetime): End date.
        limit (int, optional): Limit of files to upload. Defaults to None.
    Returns:
        list[str]: List of S3 paths where the files were uploaded.
    """
    logger.info(f"Consultando notas entre {start} e {end}...")

    gen_notas = [
        *enumerate(
            islice(
                iter_notes(
                    server=server,
                    database=database,
                    tips=tips,
                    start=start,
                    end=end,
                    font=font,
                ),
                limit,
            )
        )
    ]

    register = len(gen_notas)
    logger.info(f"Total de notas: {len(gen_notas)}")

    if not register:
        logger.warning("Nenhum registro encontrado.")
        raise ValueError("Nenhum registro encontrado.")

    client = Storage()
    if client.create_bucket(BUCKET_NAME):
        logger.info(f"Bucket {BUCKET_NAME} criado com sucesso.")

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        rst = executor.map(
            partial(upload_file, client, put_object=put_object), gen_notas
        )

    return list(rst)
