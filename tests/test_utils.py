from csvs_to_sqlite import utils
import pytest
import sqlite3
import pandas as pd

TEST_TABLES = '''
CREATE TABLE foo (
  id integer primary key,
  value text
);
'''


@pytest.mark.parametrize('table,expected', [
    ('foo', True),
    ('bar', False),
])
def test_table_exists(table, expected):
    conn = sqlite3.connect(':memory:')
    conn.executescript(TEST_TABLES)
    assert expected == utils.table_exists(conn, table)


def test_get_create_table_sql():
    df = pd.DataFrame([{'number': 1, 'letter': 'a'}])
    sql, columns = utils.get_create_table_sql('hello', df)
    assert (
        'CREATE TABLE "hello" (\n'
        '"index" INTEGER,\n'
        '  "letter" TEXT,\n'
        '  "number" INTEGER\n'
        ')'
    ) == sql
    assert ['index', 'letter', 'number'] == columns


def test_refactor_dataframes():
    df = pd.DataFrame([{
        'name': 'Terry',
        'score': 0.5,
    }, {
        'name': 'Terry',
        'score': 0.8,
    }, {
        'name': 'Owen',
        'score': 0.7,
    }])
    output = utils.refactor_dataframes([df], ['name:People:first_name'])
    assert 2 == len(output)
    lookup_table, dataframe = output
    assert {1: 'Terry', 2: 'Owen'} == lookup_table.id_to_value
    assert (
        '   name  score\n'
        '0     1    0.5\n'
        '1     1    0.8\n'
        '2     2    0.7'
    ) == str(dataframe)
