from deltalake import DeltaTable, write_deltalake
from xml_aws_athena import config
import pyarrow as pa
import duckdb
import os
from datetime import datetime
from time import sleep
import logging

logger = logging.getLogger(__name__)


region_name = config.get("region_name")
aws_access_key_id = config.get("aws_access_key_id")
aws_secret_access_key = config.get("aws_secret_access_key")
table_path = f"s3://{config.get('bucket_xml')}/silver/notas/"

storage_options = {
    "AWS_REGION": region_name,
    "AWS_ACCESS_KEY_ID": aws_access_key_id,
    "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
}


def is_delta_table(path: str = table_path) -> bool:
    """
    Check if the path is a Delta Lake table.
    """
    return DeltaTable.is_deltatable(path, storage_options=storage_options)


def write_deltalake_aws(data: pa.Table, schema: pa.Schema) -> None:
    """
    Write a Delta Lake table.
    """
    # Create a new Delta Lake table
    write_deltalake(
        table_path,
        data,
        schema=schema,
        mode="overwrite",
        storage_options=storage_options,
    )


def merge_deltalake_aws(data: pa.Table) -> None:
    """
    Merge a Delta Lake table.
    """
    try:
        dt = DeltaTable(table_path, storage_options=storage_options)
        rst = (
            dt.merge(
                data,
                predicate="s.chave = t.chave and s.item = t.item",
                source_alias="s",
                target_alias="t",
            )
            .when_not_matched_insert_all()
            .when_matched_update_all()
            .execute()
        )
    except Exception:
        logger.info("Erro ao fazer merge, tentando novamente em 5 segundos ...")
        sleep(5.0)
        merge_deltalake_aws(data)

    return rst


def delta_optimize_aws() -> list[str]:
    """Optimize Delta Lake table."""
    dt = DeltaTable(table_path, storage_options=storage_options)
    dt.optimize.compact()

    rst = dt.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
    dt.create_checkpoint()
    dt.cleanup_metadata()

    return rst


def read_deltalake_aws(start: datetime, end: datetime) -> pa.Table:
    """
    Read a Delta Lake table.
    """

    with duckdb.connect(
        database="delta.duckdb", config={"threads": os.cpu_count() * 5}
    ) as con:
        for ext in ["delta", "httpfs"]:
            con.install_extension(ext)
            con.load_extension(ext)

        # credentials
        con.sql(
            f"""
            CREATE SECRET IF NOT EXISTS (
                TYPE s3,
                KEY_ID '{aws_access_key_id}',
                SECRET '{aws_secret_access_key}',
                REGION '{region_name}'
            )
            """
        )

        arrow = con.sql(
            f"""
                select distinct on (chave, item)
                    *,
                    year(dh_emi) as "year", 
                    month(dh_emi) as "month"
                from delta_scan({table_path!r})
                where dh_emi between '{start:%Y-%m-%d} 00:00:00.000' and '{end:%Y-%m-%d} 23:59:59.999'
                """
        ).arrow()

    return arrow
