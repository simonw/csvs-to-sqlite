from click.testing import CliRunner
from csvs_to_sqlite import cli
from six import string_types
import sqlite3

CSV = '''county,precinct,office,district,party,candidate,votes
Yolo,100001,President,,LIB,Gary Johnson,41
Yolo,100001,President,,PAF,Gloria Estela La Riva,8
Yolo,100001,Proposition 51,,,No,398
Yolo,100001,Proposition 51,,,Yes,460
Yolo,100001,State Assembly,7,DEM,Kevin McCarty,572
Yolo,100001,State Assembly,7,REP,Ryan K. Brown,291'''


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
        print(result.output)
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
