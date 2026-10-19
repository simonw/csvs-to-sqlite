import sqlite3

import pytest
from click.testing import CliRunner

from csvs_to_sqlite.cli import cli


@pytest.mark.parametrize("table", [None, "renamed"])
@pytest.mark.parametrize("mode", ["create", "append", "replace"])
def test_table_name_column(tmp_path, table, mode):
    source = tmp_path / "input.csv"
    source.write_text("table_name,value\nfirst,1\nsecond,2\n", encoding="utf8")
    database = tmp_path / "output.db"
    args = [str(source), str(database)]
    if table:
        args += ["--table", table]
    runner = CliRunner()
    result = runner.invoke(cli, args)
    assert result.exit_code == 0, result.output
    expected = [("first", 1), ("second", 2)]
    if mode != "create":
        source.write_text("table_name,value\nthird,3\n", encoding="utf8")
        if mode == "replace":
            args += ["--replace-tables"]
        result = runner.invoke(cli, args)
        assert result.exit_code == 0, result.output
        expected = [("third", 3)] if mode == "replace" else expected + [("third", 3)]
    with sqlite3.connect(str(database)) as conn:
        assert (
            conn.execute(
                'select table_name, value from "{}"'.format(table or "input")
            ).fetchall()
            == expected
        )


def test_table_name_column_with_extraction(tmp_path):
    source = tmp_path / "input.csv"
    source.write_text(
        "table_name,kind\nfirst,fruit\nsecond,vegetable\n", encoding="utf8"
    )
    database = tmp_path / "output.db"
    result = CliRunner().invoke(
        cli, [str(source), str(database), "--extract-column", "kind"]
    )
    assert result.exit_code == 0, result.output
    with sqlite3.connect(str(database)) as conn:
        assert conn.execute(
            "select input.table_name, kind.value from input join kind on input.kind = kind.id"
        ).fetchall() == [("first", "fruit"), ("second", "vegetable")]


def test_filename_column_named_table_name(tmp_path):
    source = tmp_path / "input.csv"
    source.write_text("value\n42\n", encoding="utf8")
    database = tmp_path / "output.db"
    result = CliRunner().invoke(
        cli, [str(source), str(database), "--filename-column", "table_name"]
    )
    assert result.exit_code == 0, result.output
    with sqlite3.connect(str(database)) as conn:
        assert conn.execute("select table_name, value from input").fetchall() == [
            ("input", 42)
        ]
