from click.testing import CliRunner
from csvs_to_sqlite import cli
from six import string_types, text_type
import sqlite3

CSV = '''county,precinct,office,district,party,candidate,votes
Yolo,100001,President,,LIB,Gary Johnson,41
Yolo,100001,President,,PAF,Gloria Estela La Riva,8
Yolo,100001,Proposition 51,,,No,398
Yolo,100001,Proposition 51,,,Yes,460
Yolo,100001,State Assembly,7,DEM,Kevin McCarty,572
Yolo,100001,State Assembly,7,REP,Ryan K. Brown,291'''

CSV_MULTI = '''film,actor_1,actor_2
The Rock,Sean Connery,Nicolas Cage
National Treasure,Nicolas Cage,Diane Kruger
Troy,Diane Kruger,Orlando Bloom'''


def test_flat():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open('test.csv', 'w').write(CSV)
        result = runner.invoke(cli.cli, ['test.csv', 'test.db'])
        assert result.exit_code == 0
        assert result.output.strip().endswith('Created test.db from 1 CSV file')
        conn = sqlite3.connect('test.db')
        assert [
            (0, 'county', 'TEXT', 0, None, 0),
            (1, 'precinct', 'INTEGER', 0, None, 0),
            (2, 'office', 'TEXT', 0, None, 0),
            (3, 'district', 'INTEGER', 0, None, 0),
            (4, 'party', 'TEXT', 0, None, 0),
            (5, 'candidate', 'TEXT', 0, None, 0),
            (6, 'votes', 'INTEGER', 0, None, 0)
        ] == list(conn.execute('PRAGMA table_info(test)'))
        rows = conn.execute('select * from test').fetchall()
        assert [
            ('Yolo', 100001, 'President', None, 'LIB', 'Gary Johnson', 41),
            ('Yolo', 100001, 'President', None, 'PAF', 'Gloria Estela La Riva', 8),
            ('Yolo', 100001, 'Proposition 51', None, None, 'No', 398),
            ('Yolo', 100001, 'Proposition 51', None, None, 'Yes', 460),
            ('Yolo', 100001, 'State Assembly', 7, 'DEM', 'Kevin McCarty', 572),
            ('Yolo', 100001, 'State Assembly', 7, 'REP', 'Ryan K. Brown', 291)
        ] == rows
        last_row = rows[-1]
        for i, t in enumerate((string_types, int, string_types, int, string_types, string_types, int)):
            assert isinstance(last_row[i], t)


def test_extract_columns():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open('test.csv', 'w').write(CSV)
        result = runner.invoke(
            cli.cli,
            'test.csv extracted.db -c office -c district -c party -c candidate'.split()
        )
        assert result.exit_code == 0
        assert result.output.strip().endswith('Created extracted.db from 1 CSV file')
        conn = sqlite3.connect('extracted.db')
        assert [
            (0, 'county', 'TEXT', 0, None, 0),
            (1, 'precinct', 'INTEGER', 0, None, 0),
            (2, 'office', 'INTEGER', 0, None, 0),
            (3, 'district', 'INTEGER', 0, None, 0),
            (4, 'party', 'INTEGER', 0, None, 0),
            (5, 'candidate', 'INTEGER', 0, None, 0),
            (6, 'votes', 'INTEGER', 0, None, 0)
        ] == list(conn.execute('PRAGMA table_info(test)'))
        rows = conn.execute('''
            select
                county, precinct, office.value, district.value,
                party.value, candidate.value, votes
            from test
                left join office on test.office = office.id
                left join district on test.district = district.id
                left join party on test.party = party.id
                left join candidate on test.candidate = candidate.id
            order by test.rowid
        ''').fetchall()
        assert [
            ('Yolo', 100001, 'President', None, 'LIB', 'Gary Johnson', 41),
            ('Yolo', 100001, 'President', None, 'PAF', 'Gloria Estela La Riva', 8),
            ('Yolo', 100001, 'Proposition 51', None, None, 'No', 398),
            ('Yolo', 100001, 'Proposition 51', None, None, 'Yes', 460),
            ('Yolo', 100001, 'State Assembly', 7, 'DEM', 'Kevin McCarty', 572),
            ('Yolo', 100001, 'State Assembly', 7, 'REP', 'Ryan K. Brown', 291)
        ] == rows
        last_row = rows[-1]
        for i, t in enumerate((string_types, int, string_types, int, string_types, string_types, int)):
            assert isinstance(last_row[i], t)

        # Check that the various foreign key tables have the right things in them
        assert [
            (1, 'President'),
            (2, 'Proposition 51'),
            (3, 'State Assembly'),
        ] == conn.execute('select * from office').fetchall()
        assert [
            (1, 7),
        ] == conn.execute('select * from district').fetchall()
        assert [
            (1, 'LIB'),
            (2, 'PAF'),
            (3, 'DEM'),
            (4, 'REP'),
        ] == conn.execute('select * from party').fetchall()
        assert [
            (1, 'Gary Johnson'),
            (2, 'Gloria Estela La Riva'),
            (3, 'No'),
            (4, 'Yes'),
            (5, 'Kevin McCarty'),
            (6, 'Ryan K. Brown'),
        ] == conn.execute('select * from candidate').fetchall()


def test_fts():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open('test.csv', 'w').write(CSV)
        result = runner.invoke(
            cli.cli,
            'test.csv fts.db -f office -f party -f candidate'.split()
        )
        assert result.exit_code == 0
        conn = sqlite3.connect('fts.db')
        assert [
            ('Yolo', 100001, 'President', 'PAF', 'Gloria Estela La Riva'),
        ] == conn.execute('''
            select county, precinct, office, party, candidate
            from test
            where rowid in (
                select rowid from test_fts
                where test_fts match 'president gloria'
            )
        ''').fetchall()


def test_fts_error_on_missing_columns():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open('test.csv', 'w').write(CSV)
        result = runner.invoke(
            cli.cli,
            'test.csv fts.db -f badcolumn'.split()
        )
        assert result.exit_code != 0
        assert result.output.strip().endswith('FTS column "badcolumn" does not exist')


def test_fts_and_extract_columns():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open('test.csv', 'w').write(CSV)
        result = runner.invoke(
            cli.cli, (
                'test.csv fts-extracted.db -c office -c party -c candidate '
                '-f party -f candidate'
            ).split()
        )
        assert result.exit_code == 0
        conn = sqlite3.connect('fts-extracted.db')
        assert [
            ('Yolo', 100001, 'President', 'PAF', 'Gloria Estela La Riva'),
        ] == conn.execute('''
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
        ''').fetchall()


def test_fts_one_column_multiple_aliases():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open('test.csv', 'w').write(CSV_MULTI)
        result = runner.invoke(
            cli.cli, (
                'test.csv fts-extracted.db -c film '
                '-c actor_1:actors:name -c actor_2:actors:name '
                '-f film -f actor_1 -f actor_2'
            ).split()
        )
        assert result.exit_code == 0
        conn = sqlite3.connect('fts-extracted.db')
        assert [
            ('The Rock', 'Sean Connery', 'Nicolas Cage'),
            ('National Treasure', 'Nicolas Cage', 'Diane Kruger'),
            ('Troy', 'Diane Kruger', 'Orlando Bloom'),
        ] == conn.execute('''
            select
                film.value, a1.name, a2.name
            from test
                join film on test.film = film.id
                join actors a1 on test.actor_1 = a1.id
                join actors a2 on test.actor_2 = a2.id
        ''').fetchall()
        assert [
            ('National Treasure', 'Nicolas Cage', 'Diane Kruger'),
            ('Troy', 'Diane Kruger', 'Orlando Bloom'),
        ] == conn.execute('''
            select
                film.value, a1.name, a2.name
            from test
                join film on test.film = film.id
                join actors a1 on test.actor_1 = a1.id
                join actors a2 on test.actor_2 = a2.id
            where test.rowid in (
                select rowid from [test_fts] where [test_fts] match 'kruger'
            )
        ''').fetchall()


def test_shape():
    runner = CliRunner()
    with runner.isolated_filesystem():
        open('test.csv', 'w').write(CSV)
        result = runner.invoke(
            cli.cli, [
                'test.csv', 'test-reshaped.db',
                '--shape', 'county:Cty,votes:Vts(REAL)'
            ]
        )
        assert result.exit_code == 0
        conn = sqlite3.connect('test-reshaped.db')
        # Check we only have Cty and Vts columns:
        assert [
            (0, 'Cty', 'TEXT', 0, None, 0),
            (1, 'Vts', 'REAL', 0, None, 0),
        ] == conn.execute('PRAGMA table_info(test);').fetchall()
        # Now check that values are as expected:
        results = conn.execute('''
            select Cty, Vts from test
        ''').fetchall()
        assert [
            ('Yolo', 41.0),
            ('Yolo', 8.0),
            ('Yolo', 398.0),
            ('Yolo', 460.0),
            ('Yolo', 572.0),
            ('Yolo', 291.0),
        ] == results
        for city, votes in results:
            assert isinstance(city, text_type)
            assert isinstance(votes, float)
