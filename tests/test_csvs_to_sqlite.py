from click.testing import CliRunner
from csvs_to_sqlite import cli
from six import string_types, text_type
from cogapp import Cog
import sys
from io import StringIO
import pathlib
import sqlite3

CSV = """county,precinct,office,district,party,candidate,votes
Yolo,100001,President,,LIB,Gary Johnson,41
Yolo,100001,President,,PAF,Gloria Estela La Riva,8
Yolo,100001,Proposition 51,,,No,398
Yolo,100001,Proposition 51,,,Yes,460
Yolo,100001,State Assembly,7,DEM,Kevin McCarty,572
Yolo,100001,State Assembly,7,REP,Ryan K. Brown,291"""

CSV_MULTI = """film,actor_1,actor_2
The Rock,Sean Connery,Nicolas Cage
National Treasure,Nicolas Cage,Diane Kruger
Troy,Diane Kruger,Orlando Bloom"""

CSV_DATES = """headline,date,datetime
First headline,3rd May 2017,10pm on April 4 1938
Second headline,04/30/2005,5:45 10 December 2009"""

CSV_DATES_CUSTOM_FORMAT = """headline,date
Custom format,03/02/01"""

CSV_CUSTOM_PRIMARY_KEYS = """pk1,pk2,name
one,one,11
one,two,12
two,one,21"""

CSV_STRINGS_AND_DATES = """name,gross,release_date
Adaptation,22.5,6 of December in the year 2002
Face/Off,245.7,19 of June in the year 1997
The Rock,134.1,9 of June in the year 1996"""


def test_flat():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(cli.cli, ["test.csv", "test.db"])
        assert result.exit_code == 0
        assert result.output.strip().endswith("Created test.db from 1 CSV file")
        conn = sqlite3.connect("test.db")
        assert [
            (0, "county", "TEXT", 0, None, 0),
            (1, "precinct", "INTEGER", 0, None, 0),
            (2, "office", "TEXT", 0, None, 0),
            (3, "district", "INTEGER", 0, None, 0),
            (4, "party", "TEXT", 0, None, 0),
            (5, "candidate", "TEXT", 0, None, 0),
            (6, "votes", "INTEGER", 0, None, 0),
        ] == list(conn.execute("PRAGMA table_info(test)"))
        rows = conn.execute("select * from test").fetchall()
        assert [
            ("Yolo", 100001, "President", None, "LIB", "Gary Johnson", 41),
            ("Yolo", 100001, "President", None, "PAF", "Gloria Estela La Riva", 8),
            ("Yolo", 100001, "Proposition 51", None, None, "No", 398),
            ("Yolo", 100001, "Proposition 51", None, None, "Yes", 460),
            ("Yolo", 100001, "State Assembly", 7, "DEM", "Kevin McCarty", 572),
            ("Yolo", 100001, "State Assembly", 7, "REP", "Ryan K. Brown", 291),
        ] == rows
        last_row = rows[-1]
        for i, t in enumerate(
            (string_types, int, string_types, int, string_types, string_types, int)
        ):
            assert isinstance(last_row[i], t)


def test_extract_columns():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(
            cli.cli,
            "test.csv extracted.db -c office -c district -c party -c candidate".split(),
        )
        assert result.exit_code == 0
        assert result.output.strip().endswith("Created extracted.db from 1 CSV file")
        conn = sqlite3.connect("extracted.db")
        assert [
            (0, "county", "TEXT", 0, None, 0),
            (1, "precinct", "INTEGER", 0, None, 0),
            (2, "office", "INTEGER", 0, None, 0),
            (3, "district", "INTEGER", 0, None, 0),
            (4, "party", "INTEGER", 0, None, 0),
            (5, "candidate", "INTEGER", 0, None, 0),
            (6, "votes", "INTEGER", 0, None, 0),
        ] == list(conn.execute("PRAGMA table_info(test)"))
        rows = conn.execute(
            """
            select
                county, precinct, office.value, district.value,
                party.value, candidate.value, votes
            from test
                left join office on test.office = office.id
                left join district on test.district = district.id
                left join party on test.party = party.id
                left join candidate on test.candidate = candidate.id
            order by test.rowid
        """
        ).fetchall()
        assert [
            ("Yolo", 100001, "President", None, "LIB", "Gary Johnson", 41),
            ("Yolo", 100001, "President", None, "PAF", "Gloria Estela La Riva", 8),
            ("Yolo", 100001, "Proposition 51", None, None, "No", 398),
            ("Yolo", 100001, "Proposition 51", None, None, "Yes", 460),
            ("Yolo", 100001, "State Assembly", "7", "DEM", "Kevin McCarty", 572),
            ("Yolo", 100001, "State Assembly", "7", "REP", "Ryan K. Brown", 291),
        ] == rows
        last_row = rows[-1]
        for i, t in enumerate(
            (
                string_types,
                int,
                string_types,
                string_types,
                string_types,
                string_types,
                int,
            )
        ):
            assert isinstance(last_row[i], t)

        # Check that the various foreign key tables have the right things in them
        assert [
            (1, "President"),
            (2, "Proposition 51"),
            (3, "State Assembly"),
        ] == conn.execute("select * from office").fetchall()
        assert [(1, "7")] == conn.execute("select * from district").fetchall()
        assert [(1, "LIB"), (2, "PAF"), (3, "DEM"), (4, "REP")] == conn.execute(
            "select * from party"
        ).fetchall()
        assert [
            (1, "Gary Johnson"),
            (2, "Gloria Estela La Riva"),
            (3, "No"),
            (4, "Yes"),
            (5, "Kevin McCarty"),
            (6, "Ryan K. Brown"),
        ] == conn.execute("select * from candidate").fetchall()

        # Check that a FTS index was created for each extracted table
        fts_tables = [
            r[0]
            for r in conn.execute(
                """
            select name from sqlite_master
            where type='table' and name like '%_fts'
            and sql like '%USING FTS%'
        """
            ).fetchall()
        ]
        assert set(fts_tables) == {
            "office_value_fts",
            "district_value_fts",
            "party_value_fts",
            "candidate_value_fts",
        }


def test_fts():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(
            cli.cli, "test.csv fts.db -f office -f party -f candidate".split()
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("fts.db")
        assert (
            [("Yolo", 100001, "President", "PAF", "Gloria Estela La Riva")]
            == conn.execute(
                """
            select county, precinct, office, party, candidate
            from test
            where rowid in (
                select rowid from test_fts
                where test_fts match 'president gloria'
            )
        """
            ).fetchall()
        )


def test_fts_error_on_missing_columns():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(cli.cli, "test.csv fts.db -f badcolumn".split())
        assert result.exit_code != 0
        assert result.output.strip().endswith('FTS column "badcolumn" does not exist')


def test_fts_and_extract_columns():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(
            cli.cli,
            (
                "test.csv fts-extracted.db -c office -c party -c candidate "
                "-f party -f candidate"
            ).split(),
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("fts-extracted.db")
        assert (
            [("Yolo", 100001, "President", "PAF", "Gloria Estela La Riva")]
            == conn.execute(
                """
            select
                county, precinct, office.value, party.value, candidate.value
            from test
                left join office on test.office = office.id
                left join party on test.party = party.id
                left join candidate on test.candidate = candidate.id
            where test.rowid in (
                select rowid from test_fts
                where test_fts match 'paf gloria'
            )
        """
            ).fetchall()
        )


def test_fts_one_column_multiple_aliases():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV_MULTI)
        result = runner.invoke(
            cli.cli,
            (
                "test.csv fts-extracted.db -c film "
                "-c actor_1:actors:name -c actor_2:actors:name "
                "-f film -f actor_1 -f actor_2"
            ).split(),
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("fts-extracted.db")
        assert (
            [
                ("The Rock", "Sean Connery", "Nicolas Cage"),
                ("National Treasure", "Nicolas Cage", "Diane Kruger"),
                ("Troy", "Diane Kruger", "Orlando Bloom"),
            ]
            == conn.execute(
                """
            select
                film.value, a1.name, a2.name
            from test
                join film on test.film = film.id
                join actors a1 on test.actor_1 = a1.id
                join actors a2 on test.actor_2 = a2.id
        """
            ).fetchall()
        )
        assert (
            [
                ("National Treasure", "Nicolas Cage", "Diane Kruger"),
                ("Troy", "Diane Kruger", "Orlando Bloom"),
            ]
            == conn.execute(
                """
            select
                film.value, a1.name, a2.name
            from test
                join film on test.film = film.id
                join actors a1 on test.actor_1 = a1.id
                join actors a2 on test.actor_2 = a2.id
            where test.rowid in (
                select rowid from [test_fts] where [test_fts] match 'kruger'
            )
        """
            ).fetchall()
        )


def test_shape():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(
            cli.cli,
            ["test.csv", "test-reshaped.db", "--shape", "county:Cty,votes:Vts(REAL)"],
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("test-reshaped.db")
        # Check we only have Cty and Vts columns:
        assert [
            (0, "Cty", "TEXT", 0, None, 0),
            (1, "Vts", "REAL", 0, None, 0),
        ] == conn.execute("PRAGMA table_info(test);").fetchall()
        # Now check that values are as expected:
        results = conn.execute(
            """
            select Cty, Vts from test
        """
        ).fetchall()
        assert [
            ("Yolo", 41.0),
            ("Yolo", 8.0),
            ("Yolo", 398.0),
            ("Yolo", 460.0),
            ("Yolo", 572.0),
            ("Yolo", 291.0),
        ] == results
        for city, votes in results:
            assert isinstance(city, text_type)
            assert isinstance(votes, float)


def test_filename_column():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test1.csv", "w").write(CSV)
        open("test2.csv", "w").write(CSV_MULTI)
        result = runner.invoke(
            cli.cli, [".", "test-filename.db", "--filename-column", "source"]
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("test-filename.db")
        assert [("./test1",), ("./test2",)] == conn.execute(
            "select name from sqlite_master order by name"
        ).fetchall()
        # Check the source column has been added and populated
        assert [("Yolo", "Gary Johnson", 41, "./test1")] == conn.execute(
            "select county, candidate, votes, source from [./test1] limit 1"
        ).fetchall()
        assert [
            ("The Rock", "Sean Connery", "Nicolas Cage", "./test2")
        ] == conn.execute(
            "select film, actor_1, actor_2, source from [./test2] limit 1"
        ).fetchall()


def test_filename_column_with_shape():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(
            cli.cli,
            [
                "test.csv",
                "test.db",
                "--filename-column",
                "source",
                "--shape",
                "county:Cty,votes:Vts",
            ],
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("test.db")
        assert [("Yolo", 41, "test")] == conn.execute(
            "select Cty, Vts, source from test limit 1"
        ).fetchall()


def test_fixed_column():
    """
    Tests that all three fixed_column options are handled correctly.
    """
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(
            cli.cli,
            [
                "test.csv",
                "test.db",
                "--fixed-column",
                "col1",
                "foo",
                "--fixed-column",
                "col2",
                "bar",
                "--fixed-column-int",
                "col3",
                "1",
                "--fixed-column-float",
                "col4",
                "1.1",
            ],
        )
        assert result.exit_code == 0
        assert result.output.strip().endswith("Created test.db from 1 CSV file")
        conn = sqlite3.connect("test.db")
        assert [
            (0, "county", "TEXT", 0, None, 0),
            (1, "precinct", "INTEGER", 0, None, 0),
            (2, "office", "TEXT", 0, None, 0),
            (3, "district", "INTEGER", 0, None, 0),
            (4, "party", "TEXT", 0, None, 0),
            (5, "candidate", "TEXT", 0, None, 0),
            (6, "votes", "INTEGER", 0, None, 0),
            (7, "col1", "TEXT", 0, None, 0),
            (8, "col2", "TEXT", 0, None, 0),
            (9, "col3", "INTEGER", 0, None, 0),
            (10, "col4", "REAL", 0, None, 0),
        ] == list(conn.execute("PRAGMA table_info(test)"))
        rows = conn.execute("select * from test").fetchall()
        assert [
            (
                "Yolo",
                100001,
                "President",
                None,
                "LIB",
                "Gary Johnson",
                41,
                "foo",
                "bar",
                1,
                1.1,
            ),
            (
                "Yolo",
                100001,
                "President",
                None,
                "PAF",
                "Gloria Estela La Riva",
                8,
                "foo",
                "bar",
                1,
                1.1,
            ),
            (
                "Yolo",
                100001,
                "Proposition 51",
                None,
                None,
                "No",
                398,
                "foo",
                "bar",
                1,
                1.1,
            ),
            (
                "Yolo",
                100001,
                "Proposition 51",
                None,
                None,
                "Yes",
                460,
                "foo",
                "bar",
                1,
                1.1,
            ),
            (
                "Yolo",
                100001,
                "State Assembly",
                7,
                "DEM",
                "Kevin McCarty",
                572,
                "foo",
                "bar",
                1,
                1.1,
            ),
            (
                "Yolo",
                100001,
                "State Assembly",
                7,
                "REP",
                "Ryan K. Brown",
                291,
                "foo",
                "bar",
                1,
                1.1,
            ),
        ] == rows


def test_fixed_column_with_shape():
    """
    Test that fixed_column works with shape.
    """
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(
            cli.cli,
            [
                "test.csv",
                "test.db",
                "--fixed-column",
                "col1",
                "foo",
                "--fixed-column",
                "col2",
                "bar",
                "--shape",
                "county:Cty,votes:Vts",
            ],
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("test.db")
        assert [("Yolo", 41, "foo", "bar")] == conn.execute(
            "select Cty, Vts, col1, col2 from test limit 1"
        ).fetchall()


def test_shape_with_extract_columns():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(
            cli.cli,
            [
                "test.csv",
                "test.db",
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
            ],
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("test.db")
        assert (
            [("Yolo", "41", "test")]
            == conn.execute(
                """
            select
                Cty.value, Vts.value, Source.value
            from test
                left join Cty on test.Cty = Cty.id
                left join Vts on test.Vts = Vts.id
                left join Source on test.Source = Source.id
            limit 1
        """
            ).fetchall()
        )


def test_custom_indexes():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(
            cli.cli,
            ["test.csv", "test.db", "--index", "county", "-i", "party,candidate"],
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("test.db")
        assert [
            ('"test_county"', "test"),
            ('"test_party_candidate"', "test"),
        ] == conn.execute(
            'select name, tbl_name from sqlite_master where type = "index" order by name'
        ).fetchall()


def test_dates_and_datetimes():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV_DATES)
        result = runner.invoke(
            cli.cli, ["test.csv", "test.db", "-d", "date", "-dt", "datetime"]
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("test.db")
        expected = [
            ("First headline", "2017-05-03", "1938-04-04T22:00:00"),
            ("Second headline", "2005-04-30", "2009-12-10T05:45:00"),
        ]
        actual = conn.execute("select * from test").fetchall()
        assert expected == actual


def test_dates_custom_formats():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV_DATES_CUSTOM_FORMAT)
        result = runner.invoke(
            cli.cli, ["test.csv", "test.db", "-d", "date", "-df", "%y/%d/%m"]
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("test.db")
        # Input was 03/02/01
        expected = [("Custom format", "2003-01-02")]
        actual = conn.execute("select * from test").fetchall()
        assert expected == actual


def test_extract_cols_no_fts():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(
            cli.cli,
            (
                "test.csv fts-extracted.db -c office -c party -c candidate "
                "-f party -f candidate --no-fulltext-fks"
            ).split(),
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("fts-extracted.db")
        assert (
            [("test_fts",)]
            == conn.execute(
                """
            select name from sqlite_master
            where type='table' and name like '%_fts'
            and sql like '%USING FTS%'
        """
            ).fetchall()
        )


def test_custom_primary_keys():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("pks.csv", "w").write(CSV_CUSTOM_PRIMARY_KEYS)
        result = runner.invoke(
            cli.cli, ("pks.csv pks.db -pk pk1 --primary-key pk2").split()
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("pks.db")
        pks = [
            r[1] for r in conn.execute('PRAGMA table_info("pks")').fetchall() if r[-1]
        ]
        assert ["pk1", "pk2"] == pks


def test_just_strings_default():
    """
    Just like test_flat(), except all columns are strings
    """
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(
            cli.cli,
            "test.csv just-strings.db --just-strings".split(),
        )
        assert result.exit_code == 0

        conn = sqlite3.connect("just-strings.db")
        assert [
            (0, "county", "TEXT", 0, None, 0),
            (1, "precinct", "TEXT", 0, None, 0),
            (2, "office", "TEXT", 0, None, 0),
            (3, "district", "TEXT", 0, None, 0),
            (4, "party", "TEXT", 0, None, 0),
            (5, "candidate", "TEXT", 0, None, 0),
            (6, "votes", "TEXT", 0, None, 0),
        ] == list(conn.execute("PRAGMA table_info(test)"))
        rows = conn.execute("select * from test").fetchall()
        assert [
            ("Yolo", "100001", "President", None, "LIB", "Gary Johnson", "41"),
            ("Yolo", "100001", "President", None, "PAF", "Gloria Estela La Riva", "8"),
            ("Yolo", "100001", "Proposition 51", None, None, "No", "398"),
            ("Yolo", "100001", "Proposition 51", None, None, "Yes", "460"),
            ("Yolo", "100001", "State Assembly", "7", "DEM", "Kevin McCarty", "572"),
            ("Yolo", "100001", "State Assembly", "7", "REP", "Ryan K. Brown", "291"),
        ] == rows
        last_row = rows[-1]
        for i, t in enumerate(
            (
                string_types,
                string_types,
                string_types,
                string_types,
                string_types,
                string_types,
                string_types,
            )
        ):
            assert isinstance(last_row[i], t)


def test_just_strings_with_shape():
    """
    Make sure shape and just_strings play well together
    """
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("test.csv", "w").write(CSV)
        result = runner.invoke(
            cli.cli,
            [
                "test.csv",
                "test-reshaped-strings.db",
                "--just-strings",
                "--shape",
                "county:Cty,district:district,votes:Vts(REAL)",
            ],
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("test-reshaped-strings.db")
        # Check that Cty, Vts exist as defined, and so does votetxt:
        assert [
            (0, "Cty", "TEXT", 0, None, 0),
            (1, "district", "TEXT", 0, None, 0),
            (2, "Vts", "REAL", 0, None, 0),
        ] == conn.execute("PRAGMA table_info(test);").fetchall()
        # Now check that values are as expected:
        results = conn.execute(
            """
            select Cty, Vts, district from test
        """
        ).fetchall()
        assert [
            ("Yolo", 41.0, None),
            ("Yolo", 8.0, None),
            ("Yolo", 398.0, None),
            ("Yolo", 460.0, None),
            ("Yolo", 572.0, "7"),
            ("Yolo", 291.0, "7"),
        ] == results
        for city, votes, district in results:
            assert isinstance(city, text_type)
            assert isinstance(votes, float)
            assert isinstance(district, text_type) or district is None


def test_just_strings_with_date_specified():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open("nic_cages_greatest.csv", "w").write(CSV_STRINGS_AND_DATES)
        result = runner.invoke(
            cli.cli,
            [
                "nic_cages_greatest.csv",
                "movies.db",
                "--date",
                "release_date",
                "--datetime-format",
                "%d of %B in the year %Y",
                "--just-strings",
            ],
        )
        assert result.exit_code == 0
        conn = sqlite3.connect("movies.db")
        expected = [
            ("Adaptation", "22.5", "2002-12-06"),
            ("Face/Off", "245.7", "1997-06-19"),
            ("The Rock", "134.1", "1996-06-09"),
        ]
        actual = conn.execute("select * from nic_cages_greatest").fetchall()
        assert expected == actual

        for name, gross, dt in actual:
            assert isinstance(gross, text_type)


def test_if_cog_needs_to_be_run():
    _stdout = sys.stdout
    sys.stdout = StringIO()
    readme = pathlib.Path(__file__).parent.parent / "README.md"
    result = Cog().main(["cog", str(readme)])
    output = sys.stdout.getvalue()
    sys.stdout = _stdout
    assert (
        output == readme.read_text()
    ), "Run 'cog -r README.md' to update help in README"
