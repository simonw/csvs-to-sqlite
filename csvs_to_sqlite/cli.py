from __future__ import absolute_import

import click
from .utils import (
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
@click.option('--replace-tables', is_flag=True, help='Replace tables if they already exist')
def cli(paths, dbname, replace_tables):
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

    db_exists = os.path.exists(dbname)

    conn = sqlite3.connect(dbname)
    csvs = csvs_from_paths(paths)
    for name, path in csvs.items():
        try:
            df = load_csv(path)
            df.to_sql(
                name,
                conn,
                if_exists='replace' if replace_tables else 'fail'
            )
        except LoadCsvError as e:
            click.echo('Could not load {}: {}'.format(
                path, e
            ), err=True)
    conn.close()

    if db_exists:
        click.echo('Added {} CSV file{} to {}'.format(
            len(csvs), '' if len(csvs) == 1 else 's', dbname
        ))
    else:
        click.echo('Created {} from {} CSV file{}'.format(
            dbname, len(csvs), '' if len(csvs) == 1 else 's'
        ))
