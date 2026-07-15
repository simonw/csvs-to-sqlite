"""
Behavior parity tests against csvs-to-sqlite 1.3.1 - the last release built
on pandas/numpy.

Each case runs the same CLI invocation twice: once against the old release
(installed on the fly in an isolated environment using
``uv run --with csvs-to-sqlite==1.3.1``) and once against the code in this
repository, then asserts that the resulting SQLite databases are equivalent:
same tables, same columns and column types, same rows, same indexes, and -
for full-text search - the same query results.

Requires ``uv`` on the PATH and (on first run) network access to install
the old release; otherwise the whole module is skipped.
"""

import shutil
import sqlite3
import subprocess
import sys

import pytest

OLD_VERSION = "1.3.1"
OLD_COMMAND = [
    "uv",
    "run",
    "--no-project",
    "--with",
    "csvs-to-sqlite==" + OLD_VERSION,
    "csvs-to-sqlite",
]


def _old_version_available():
    if shutil.which("uv") is None:
        return False
    try:
        result = subprocess.run(
            OLD_COMMAND + ["--version"],
            capture_output=True,
            text=True,
            timeout=600,
        )
    except (subprocess.TimeoutExpired, OSError):
        return False
    return result.returncode == 0 and OLD_VERSION in result.stdout


pytestmark = pytest.mark.skipif(
    not _old_version_available(),
    reason="uv run --with csvs-to-sqlite=={} is not available".format(OLD_VERSION),
)


VOTES_CSV = """county,precinct,office,district,party,candidate,votes
Yolo,100001,President,,LIB,Gary Johnson,41
Yolo,100001,President,,PAF,Gloria Estela La Riva,8
Yolo,100001,Proposition 51,,,No,398
Yolo,100001,Proposition 51,,,Yes,460
Yolo,100001,State Assembly,7,DEM,Kevin McCarty,572
Yolo,100001,State Assembly,7,REP,Ryan K. Brown,291"""

MULTI_CSV = """film,actor_1,actor_2
The Rock,Sean Connery,Nicolas Cage
National Treasure,Nicolas Cage,Diane Kruger
Troy,Diane Kruger,Orlando Bloom"""

DATES_CSV = """headline,date,datetime
First headline,3rd May 2017,10pm on April 4 1938
Second headline,04/30/2005,5:45 10 December 2009"""

PKS_CSV = """pk1,pk2,name
one,one,11
one,two,12
two,one,21"""

# Exercises the pandas type inference quirks the rewrite has to preserve:
# integer-valued float columns become INTEGER, NA words become NULL,
# boolean-ish columns become INTEGER 1/0, whitespace-padded numbers parse,
# scientific notation, leading zeros in mixed columns survive as text, etc.
TYPES_CSV = """id,int_blank,float_int,floats,mixed,na_words,bools,empty,leading_zero,exp,neg,mixed_num,ws_int,inf_col
1,10,1.0,1.5,1,NA,True,,007,1e3,-5,1, 41,inf
2,,2.0,2.5,2.5,null,False,,4,2e3,-6,2.5,42 ,-inf
3,30,3.0,3.5,three,n/a,true,,x,3e3,-7,3,43,inf"""

BOOL_BLANKS_CSV = """id,bool_blank,true_only,mixed_case
1,True,True,TRUE
2,,True,FALSE"""

QUOTED_CSV = '''id,text,number
1,"Hello, world","1,000"
2,"She said ""hi""",2'''

BAD_LINES_CSV = """a,b,c
1,2,3
4,5,6,7
8,9,10"""

SEMICOLON_CSV = """a;b;c
1;hello;2.5
2;world;3.5"""

LATIN1_CSV_BYTES = "name,city\nJosé,México\nRémy,Montréal\n".encode("latin-1")


def _case(name, files, invocations, queries=None):
    return pytest.param(files, invocations, queries or [], id=name)


CASES = [
    _case("flat", {"test.csv": VOTES_CSV}, [["test.csv", "out.db"]]),
    _case(
        "extract_columns",
        {"test.csv": VOTES_CSV},
        [
            [
                "test.csv",
                "out.db",
                "-c",
                "office",
                "-c",
                "district",
                "-c",
                "party",
                "-c",
                "candidate",
            ]
        ],
    ),
    _case(
        "fts",
        {"test.csv": VOTES_CSV},
        [["test.csv", "out.db", "-f", "office", "-f", "party", "-f", "candidate"]],
        ["select rowid from test_fts where test_fts match 'president gloria'"],
    ),
    _case(
        "fts_and_extract",
        {"test.csv": VOTES_CSV},
        [
            [
                "test.csv",
                "out.db",
                "-c",
                "office",
                "-c",
                "party",
                "-c",
                "candidate",
                "-f",
                "party",
                "-f",
                "candidate",
            ]
        ],
        ["select rowid from test_fts where test_fts match 'paf gloria'"],
    ),
    _case(
        "fts_extract_aliases",
        {"test.csv": MULTI_CSV},
        [
            [
                "test.csv",
                "out.db",
                "-c",
                "film",
                "-c",
                "actor_1:actors:name",
                "-c",
                "actor_2:actors:name",
                "-f",
                "film",
                "-f",
                "actor_1",
                "-f",
                "actor_2",
            ]
        ],
        ["select rowid from test_fts where test_fts match 'kruger'"],
    ),
    _case(
        "extract_no_fulltext_fks",
        {"test.csv": VOTES_CSV},
        [
            [
                "test.csv",
                "out.db",
                "-c",
                "office",
                "-c",
                "party",
                "--no-fulltext-fks",
            ]
        ],
    ),
    _case(
        "extract_no_index_fks",
        {"test.csv": VOTES_CSV},
        [["test.csv", "out.db", "-c", "office", "-c", "party", "--no-index-fks"]],
    ),
    _case(
        "shape",
        {"test.csv": VOTES_CSV},
        [["test.csv", "out.db", "--shape", "county:Cty,votes:Vts(REAL)"]],
    ),
    _case(
        "shape_with_extract",
        {"test.csv": VOTES_CSV},
        [
            [
                "test.csv",
                "out.db",
                "--filename-column",
                "Source",
                "--shape",
                "county:Cty,votes:Vts",
                "-c",
                "Cty",
                "-c",
                "Vts",
                "-c",
                "Source",
            ]
        ],
    ),
    _case(
        "just_strings",
        {"test.csv": VOTES_CSV},
        [["test.csv", "out.db", "--just-strings"]],
    ),
    _case(
        "just_strings_shape",
        {"test.csv": VOTES_CSV},
        [
            [
                "test.csv",
                "out.db",
                "--just-strings",
                "--shape",
                "county:Cty,district:district,votes:Vts(REAL)",
            ]
        ],
    ),
    _case(
        "filename_and_fixed_columns",
        {"test1.csv": VOTES_CSV, "test2.csv": MULTI_CSV},
        [
            [
                ".",
                "out.db",
                "--filename-column",
                "source",
                "--fixed-column",
                "col1",
                "foo",
                "--fixed-column-int",
                "col3",
                "1",
                "--fixed-column-float",
                "col4",
                "1.1",
            ]
        ],
    ),
    _case(
        "csvs_in_directories",
        {"test1.csv": VOTES_CSV, "sub/test2.csv": MULTI_CSV},
        [[".", "out.db"]],
    ),
    _case(
        "primary_keys",
        {"pks.csv": PKS_CSV},
        [["pks.csv", "out.db", "-pk", "pk1", "-pk", "pk2"]],
    ),
    _case(
        "dates",
        {"test.csv": DATES_CSV},
        [["test.csv", "out.db", "-d", "date", "-dt", "datetime"]],
    ),
    _case(
        "dates_custom_format",
        {"test.csv": "headline,date\nCustom format,03/02/01"},
        [["test.csv", "out.db", "-d", "date", "-df", "%y/%d/%m"]],
    ),
    _case(
        "custom_indexes",
        {"test.csv": VOTES_CSV},
        [["test.csv", "out.db", "-i", "county", "-i", "party,candidate"]],
    ),
    _case(
        "custom_table_name",
        {"test.csv": VOTES_CSV},
        [["test.csv", "out.db", "-t", "custom"]],
    ),
    _case("type_inference", {"types.csv": TYPES_CSV}, [["types.csv", "out.db"]]),
    _case(
        "bools_with_blanks",
        {"bools.csv": BOOL_BLANKS_CSV},
        [["bools.csv", "out.db"]],
    ),
    _case("quoting", {"quoted.csv": QUOTED_CSV}, [["quoted.csv", "out.db"]]),
    _case(
        "skip_errors",
        {"bad.csv": BAD_LINES_CSV},
        [["bad.csv", "out.db", "--skip-errors"]],
    ),
    _case(
        "separator",
        {"semi.csv": SEMICOLON_CSV},
        [["semi.csv", "out.db", "-s", ";"]],
    ),
    _case("latin1", {"test.csv": LATIN1_CSV_BYTES}, [["test.csv", "out.db"]]),
    _case(
        "append_second_run",
        {"test.csv": VOTES_CSV},
        [["test.csv", "out.db"], ["test.csv", "out.db"]],
    ),
    _case(
        "replace_tables_second_run",
        {"test.csv": VOTES_CSV},
        [["test.csv", "out.db"], ["test.csv", "out.db", "--replace-tables"]],
    ),
    _case(
        "extract_second_run",
        {"test.csv": VOTES_CSV},
        [
            ["test.csv", "out.db", "-c", "office", "-c", "candidate"],
            ["test.csv", "out.db", "-c", "office", "-c", "candidate"],
        ],
    ),
]


def _comparable_stdout(stdout):
    # The "Loaded N dataframes" progress line was deliberately reworded
    # after 1.3.1, so it is excluded from the comparison
    return [line for line in stdout.splitlines() if not line.startswith("Loaded ")]


def _write_files(directory, files):
    for relpath, content in files.items():
        path = directory / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            path.write_bytes(content)
        else:
            path.write_text(content)


def _run_old(args, cwd):
    return subprocess.run(
        OLD_COMMAND + args,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=600,
    )


def _run_new(args, cwd):
    return subprocess.run(
        [sys.executable, "-c", "from csvs_to_sqlite.cli import cli; cli()"] + args,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=600,
    )


def _summarize(db_path):
    """Everything about a database we expect to match between versions.

    FTS shadow tables (test_fts_data, test_fts_idx, ...) are excluded from
    the row comparison because their binary content legitimately varies
    between SQLite builds - FTS equivalence is asserted by running the same
    match queries against both databases instead.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        master = conn.execute(
            "select type, name, coalesce(sql, '') from sqlite_master order by name"
        ).fetchall()
        fts_tables = {
            name
            for (type_, name, sql) in master
            if type_ == "table" and "using fts" in sql.lower()
        }

        def is_shadow(name):
            return any(name.startswith(fts + "_") for fts in fts_tables)

        summary = {
            "fts_tables": sorted(fts_tables),
            "indexes": sorted(
                name for (type_, name, sql) in master if type_ == "index"
            ),
            "tables": {},
        }
        for type_, name, sql in master:
            if type_ != "table" or is_shadow(name) or name.startswith("sqlite_"):
                continue
            columns = conn.execute("PRAGMA table_info([{}])".format(name)).fetchall()
            table = {"columns": columns}
            if name not in fts_tables:
                table["rows"] = conn.execute(
                    "select * from [{}] order by rowid".format(name)
                ).fetchall()
            summary["tables"][name] = table
        return summary
    finally:
        conn.close()


@pytest.mark.parametrize("files,invocations,queries", CASES)
def test_parity_with_old_version(tmp_path, files, invocations, queries):
    old_dir = tmp_path / "old"
    new_dir = tmp_path / "new"
    old_dir.mkdir()
    new_dir.mkdir()
    _write_files(old_dir, files)
    _write_files(new_dir, files)

    for args in invocations:
        old_result = _run_old(args, old_dir)
        new_result = _run_new(args, new_dir)
        assert (
            old_result.returncode == new_result.returncode
        ), "exit codes differ.\nold stderr: {}\nnew stderr: {}".format(
            old_result.stderr, new_result.stderr
        )
        assert _comparable_stdout(old_result.stdout) == _comparable_stdout(
            new_result.stdout
        )

    old_summary = _summarize(old_dir / "out.db")
    new_summary = _summarize(new_dir / "out.db")
    assert old_summary == new_summary

    for query in queries:
        old_conn = sqlite3.connect(str(old_dir / "out.db"))
        new_conn = sqlite3.connect(str(new_dir / "out.db"))
        assert (
            old_conn.execute(query).fetchall() == new_conn.execute(query).fetchall()
        ), query
        old_conn.close()
        new_conn.close()
