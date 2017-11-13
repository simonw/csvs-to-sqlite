import click
from utils import (
    LoadCsvError,
    csvs_from_paths,
    load_csv,
)
import os
import sqlite3


@click.command()
@click.argument(
    'paths',
    type=click.Path(exists=True),
    nargs=-1,
    required=True,
)
@click.argument('dbname', nargs=1)
def cli(paths, dbname):
    """
    PATHS: paths to individual .csv files or to directories containing .csvs

    DBNAME: name of the SQLite database file to create
    """
    if dbname.endswith('.csv'):
        raise click.BadParameter(
            'dbname must not end with .csv'
        )
    if '.' not in dbname:
        dbname += '.db'

    if os.path.exists(dbname):
        raise click.BadParameter(
            '{} already exists!'.format(dbname)
        )

    conn = sqlite3.connect(dbname)
    csvs = csvs_from_paths(paths)
    for name, path in csvs.items():
        try:
            df = load_csv(path)
            df.to_sql(name, conn)
        except LoadCsvError, e:
            click.echo('Could not load {}: {}'.format(
                path, e
            ), err=True)
    conn.close()
    click.echo('Created {} from {} csv file{}'.format(
        dbname, len(csvs), '' if len(csvs) == 1 else 's'
    ))
