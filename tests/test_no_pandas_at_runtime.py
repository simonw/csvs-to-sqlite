"""
Red/green TDD tests for the sqlite-utils rewrite.

These tests fail against the original pandas/numpy implementation ("red")
and pass once csvs-to-sqlite runs on sqlite-utils instead ("green").

numpy and pandas remain installed as *test* dependencies so that the
original test suite - which builds pandas DataFrames directly - keeps
running, but the installed package itself must never import them.
"""

import sqlite3
import subprocess
import sys
import textwrap

CSV = """county,precinct,office,district,party,candidate,votes
Yolo,100001,President,,LIB,Gary Johnson,41
Yolo,100001,President,,PAF,Gloria Estela La Riva,8
Yolo,100001,Proposition 51,,,No,398
Yolo,100001,Proposition 51,,,Yes,460
Yolo,100001,State Assembly,7,DEM,Kevin McCarty,572
Yolo,100001,State Assembly,7,REP,Ryan K. Brown,291"""

CSV_DATES = """headline,date,datetime
First headline,3rd May 2017,10pm on April 4 1938
Second headline,04/30/2005,5:45 10 December 2009"""

# Prepended to every script below: any attempt to import pandas or numpy
# (directly or transitively) raises ImportError.
BLOCKER = textwrap.dedent(
    """
    import sys

    class _BlockPandasNumpy:
        BLOCKED = {"pandas", "numpy"}

        def find_spec(self, name, path=None, target=None):
            if name.partition(".")[0] in self.BLOCKED:
                raise ImportError(
                    "import of {} is blocked: csvs-to-sqlite must not "
                    "need it at runtime".format(name)
                )

    sys.meta_path.insert(0, _BlockPandasNumpy())
    """
)


def run_with_pandas_blocked(code, cwd):
    return subprocess.run(
        [sys.executable, "-c", BLOCKER + textwrap.dedent(code)],
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )


def test_import_does_not_require_pandas_or_numpy(tmp_path):
    result = run_with_pandas_blocked("import csvs_to_sqlite.cli", tmp_path)
    assert result.returncode == 0, result.stderr


def test_cli_runs_without_pandas_or_numpy(tmp_path):
    (tmp_path / "test.csv").write_text(CSV)
    code = """
    from click.testing import CliRunner
    from csvs_to_sqlite import cli

    runner = CliRunner()
    result = runner.invoke(
        cli.cli,
        [
            "test.csv", "test.db",
            "-c", "office", "-c", "party", "-c", "candidate",
            "-f", "candidate",
            "-i", "county",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    """
    result = run_with_pandas_blocked(code, tmp_path)
    assert result.returncode == 0, result.stderr
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    assert [
        ("Yolo", 100001, "President", None, "LIB", "Gary Johnson", 41)
    ] == conn.execute(
        """
        select county, precinct, office.value, district, party.value,
            candidate.value, votes
        from test
            left join office on test.office = office.id
            left join party on test.party = party.id
            left join candidate on test.candidate = candidate.id
        limit 1
        """
    ).fetchall()


def test_dates_work_without_pandas_or_numpy(tmp_path):
    (tmp_path / "test.csv").write_text(CSV_DATES)
    code = """
    from click.testing import CliRunner
    from csvs_to_sqlite import cli

    runner = CliRunner()
    result = runner.invoke(
        cli.cli,
        ["test.csv", "test.db", "-d", "date", "-dt", "datetime"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    """
    result = run_with_pandas_blocked(code, tmp_path)
    assert result.returncode == 0, result.stderr
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    assert [
        ("First headline", "2017-05-03", "1938-04-04T22:00:00"),
        ("Second headline", "2005-04-30", "2009-12-10T05:45:00"),
    ] == conn.execute("select * from test").fetchall()
