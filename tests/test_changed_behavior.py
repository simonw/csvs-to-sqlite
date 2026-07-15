"""
Tests for deliberate behavior changes since the pandas-based 1.3.1 release.

These are the places where the rewrite intentionally does NOT match the
old version, so they are excluded from the parity tests:

- the "Loaded N dataframes" progress message now says "CSV file(s)"
- integer columns too large for SQLite's 64-bit integers are stored as
  TEXT instead of crashing with OverflowError (as 1.3.1 did)
"""

import sqlite3

from click.testing import CliRunner

from csvs_to_sqlite import cli

CSV = """county,precinct,votes
Yolo,100001,41
Yolo,100002,8"""

CSV_TWO = """film,actor
The Rock,Sean Connery"""

# 9223372036854775807 is SQLite's maximum integer; anything bigger cannot
# be stored as an integer at all
CSV_BIG_INTS = """id,big,big_blank,normal
1,9223372036854775807,,10
2,9223372036854775808,18446744073709551616,20"""


def test_loaded_message_no_longer_mentions_dataframes():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(cli.cli, ["test.csv", "test.db"])
        assert result.exit_code == 0
        assert "dataframes" not in result.output
        assert "Loaded 1 CSV file\n" in result.output


def test_loaded_message_is_pluralized():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test1.csv", "w").write(CSV)
        open("test2.csv", "w").write(CSV_TWO)
        result = runner.invoke(cli.cli, ["test1.csv", "test2.csv", "test.db"])
        assert result.exit_code == 0
        assert "Loaded 2 CSV files\n" in result.output


def test_integers_too_big_for_sqlite_are_kept_as_text():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("big.csv", "w").write(CSV_BIG_INTS)
        result = runner.invoke(cli.cli, ["big.csv", "big.db"])
        assert result.exit_code == 0, result.output
        conn = sqlite3.connect("big.db")
        assert [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "big", "TEXT", 0, None, 0),
            (2, "big_blank", "TEXT", 0, None, 0),
            (3, "normal", "INTEGER", 0, None, 0),
        ] == conn.execute("PRAGMA table_info(big)").fetchall()
        assert [
            (1, "9223372036854775807", None, 10),
            (2, "9223372036854775808", "18446744073709551616", 20),
        ] == conn.execute("select * from big order by rowid").fetchall()


def test_max_and_min_sqlite_integers_still_stored_as_integers():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("limits.csv", "w").write(
            "id,maxed,minned\n1,9223372036854775807,-9223372036854775808"
        )
        result = runner.invoke(cli.cli, ["limits.csv", "limits.db"])
        assert result.exit_code == 0, result.output
        conn = sqlite3.connect("limits.db")
        assert [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "maxed", "INTEGER", 0, None, 0),
            (2, "minned", "INTEGER", 0, None, 0),
        ] == conn.execute("PRAGMA table_info(limits)").fetchall()
        assert [(1, 9223372036854775807, -9223372036854775808)] == conn.execute(
            "select * from limits"
        ).fetchall()
