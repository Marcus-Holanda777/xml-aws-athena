from athena_mvsh import Athena, CursorParquetDuckdb
import pyarrow as pa
from xml_aws_athena import config
import logging

logger = logging.getLogger(__name__)


def comand_gold(data: pa.Table) -> None:
    """
    Write the final table to Athena.
    Args:
        data (pa.Table): Data to write.
    """

    cursor = CursorParquetDuckdb(config.get("s3_staging_dir"), result_reuse_enable=True)

    table_name = "notas_xml"
    schema = "prevencao-perdas"

    location = f"{config.get('location_table')}{table_name}/"

    logger.info(f"Escrevendo tabela {table_name} no Athena...")
    logger.info(f"Total de registros {data.shape}")

    with Athena(cursor=cursor) as cliente:
        is_table = cliente.execute(f"""
            select 1 as ok from information_schema.tables
            where table_schema = '{schema}'
            and table_name = '{table_name}' limit 1
        """)

        if is_table.fetchone():
            logger.info(f"Tabela {table_name} já existe, atualizando...")
            cliente.merge_table_iceberg(
                table_name,
                data,
                schema=schema,
                predicate="t.chave = s.chave and t.item = s.item",
                location=location,
            )
        else:
            logger.info(f"Criando tabela {table_name}...")
            cliente.write_table_iceberg(
                data, table_name=table_name, schema=schema, location=location
            )

        cliente.execute(f"OPTIMIZE {schema}.{table_name} REWRITE DATA USING BIN_PACK")
        cliente.execute(f"VACUUM {schema}.{table_name}")
