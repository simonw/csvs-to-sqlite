from csvs_to_sqlite import utils
import pytest
import sqlite3
import pandas as pd

TEST_TABLES = """
CREATE TABLE foo (
  id integer primary key,
  value text
);
"""


@pytest.mark.parametrize("table,expected", [("foo", True), ("bar", False)])
def test_table_exists(table, expected):
    conn = sqlite3.connect(":memory:")
    conn.executescript(TEST_TABLES)
    assert expected == utils.table_exists(conn, table)


def test_get_create_table_sql():
    df = pd.DataFrame([{"number": 1, "letter": "a"}])
    sql, columns = utils.get_create_table_sql("hello", df)
    assert (
        'CREATE TABLE "hello" (\n'
        '"index" INTEGER,\n'
        '  "number" INTEGER,\n'
        '  "letter" TEXT\n'
        ")"
    ) == sql
    assert {"index", "letter", "number"} == set(columns)


def test_refactor_dataframes():
    df = pd.DataFrame(
        [
            {"name": "Terry", "score": 0.5},
            {"name": "Terry", "score": 0.8},
            {"name": "Owen", "score": 0.7},
        ]
    )
    conn = sqlite3.connect(":memory:")
    output = utils.refactor_dataframes(
        conn, [df], {"name": ("People", "first_name")}, False
    )
    assert 1 == len(output)
    dataframe = output[0]
    # There should be a 'People' table in sqlite
    assert [(1, "Terry"), (2, "Owen")] == conn.execute(
        "select id, first_name from People"
    ).fetchall()
    assert (
        "   name  score\n" "0     1    0.5\n" "1     1    0.8\n" "2     2    0.7"
    ) == str(dataframe)
