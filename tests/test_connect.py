from xml_aws_athena.connect import iter_notes
from datetime import datetime
from itertools import islice
from pyodbc import Row
from xml_aws_athena import config


def test_iter_notes():
    # Define test parameters
    server = config.get("server_test")
    database = config.get("database_test")
    tips = ["INCINERACAO", "ESTORNO-INCINERACAO"]
    start = datetime(2025, 4, 1)
    end = datetime(2025, 4, 2)

    # Call the function with the test parameters
    result = list(
        islice(iter_notes(server, database, tips=tips, start=start, end=end), 10)
    )

    # Assert that the result is a list
    assert isinstance(result, list)

    # Assert that the result contains tuples with expected length
    for row in result:
        assert isinstance(row, Row), f"Esperava uma Row, mas recebeu {type(row)}"
        assert len(row) == 5
