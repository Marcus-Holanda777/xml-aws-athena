from contextlib import contextmanager
import pyodbc
from datetime import datetime


@contextmanager
def do_connect(*args, **kwargs):
    con = pyodbc.connect(*args, **kwargs)
    con.autocommit = True

    con.set_attr(pyodbc.SQL_ATTR_TXN_ISOLATION, pyodbc.SQL_TXN_READ_UNCOMMITTED)

    con.autocommit = False
    cursor = con.cursor()

    try:
        yield cursor
    except Exception:
        con.rollback()
        raise
    else:
        con.commit()
    finally:
        cursor.close()
        con.close()


def iter_notes(
    server,
    database,
    *,
    tips: list[str],
    start: datetime,
    end: datetime,
    font: str = "dbnfe",
):
    DRIVER = (
        "Driver={ODBC Driver 18 for Sql Server};"
        f"Server={server};"
        f"Database={database};"
        "TrustServerCertificate=Yes;"
        "Authentication=ActiveDirectoryIntegrated;"
    )

    query = """
    select dscXml, codChaveAcesso, isnStatus, dthGravacao, controle1 
    from {font}.dbo.tbNfeXml 
    where controle1 in({tips}) and isnstatus is not null and codChaveAcesso is not null 
    and dthGravacao between '{start:%Y-%m-%d} 00:00:00.000' and '{end:%Y-%m-%d} 23:59:59.999' 
    and convert(VARCHAR(MAX), dscXml) != '' """

    params = {
        "tips": ",".join(f"{c!r}" for c in tips),
        "start": start,
        "end": end,
        "font": font,
    }

    with do_connect(DRIVER) as cursor:
        cursor.execute(query.format(**params))

        for row in cursor:
            yield row
