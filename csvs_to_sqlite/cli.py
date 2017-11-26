from __future__ import absolute_import

import click
from .utils import (
    LoadCsvError,
    LookupTable,
    best_fts_version,
    csvs_from_paths,
    generate_and_populate_fts,
    load_csv,
    refactor_dataframes,
    table_exists,
    drop_table,
    to_sql_with_foreign_keys,
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
@click.option('--separator', '-s', default=',', help='Field separator in input .csv')
@click.option('--replace-tables', is_flag=True, help='Replace tables if they already exist')
@click.option('--extract-column', '-c', multiple=True, help=(
    "One or more columns to 'extract' into a separate lookup table. "
    "If you pass a simple column name that column will be replaced "
    "with integer foreign key references to a new table of that "
    "name. You can customize the name of the table like so:\n\n\n"
    "   \b\n"
    "    --extract-column state:States:state_name"
    "   \b\n"
    "This will pull unique values from the 'state' column and use "
    "them to populate a new 'States' table, with an id column "
    "primary key and a state_name column containing the strings "
    "from the original column."
))
@click.option('--fts', '-f', multiple=True, help=(
    "One or more columns to use to populate a full-text index"
))
@click.version_option()
def cli(paths, dbname, separator, replace_tables, extract_column, fts):
    """
    PATHS: paths to individual .csv files or to directories containing .csvs

    DBNAME: name of the SQLite database file to create
    """
    # make plural for more readable code:
    extract_columns = extract_column
    del extract_column

    click.echo('extract_columns={}'.format(extract_columns))
    if dbname.endswith('.csv'):
        raise click.BadParameter(
            'dbname must not end with .csv'
        )
    if '.' not in dbname:
        dbname += '.db'

    db_existed = os.path.exists(dbname)

    conn = sqlite3.connect(dbname)

    dataframes = []
    csvs = csvs_from_paths(paths)
    for name, path in csvs.items():
        try:
            df = load_csv(path, separator)
            df.table_name = name
            dataframes.append(df)
        except LoadCsvError as e:
            click.echo('Could not load {}: {}'.format(
                path, e
            ), err=True)

    click.echo('Loaded {} dataframes'.format(len(dataframes)))

    # Use extract_columns to build a column:(table,label) dictionary
    foreign_keys = {}
    for col in extract_columns:
        bits = col.split(':')
        if len(bits) == 3:
            foreign_keys[bits[0]] = (bits[1], bits[2])
        elif len(bits) == 2:
            foreign_keys[bits[0]] = (bits[1], 'value')
        else:
            foreign_keys[bits[0]] = (bits[0], 'value')

    # Now we have loaded the dataframes, we can refactor them
    created_tables = {}
    refactored = refactor_dataframes(dataframes, foreign_keys)
    for df in refactored:
        if isinstance(df, LookupTable):
            df.to_sql(
                df.table_name, conn
            )
        else:
            # This is a bit trickier because we need to
            # create the table with extra SQL for foreign keys
            if replace_tables and table_exists(conn, df.table_name):
                drop_table(conn, df.table_name)
            to_sql_with_foreign_keys(
                conn, df, df.table_name, foreign_keys
            )
            created_tables[df.table_name] = df

    # Create FTS tables
    if fts:
        fts_version = best_fts_version()
        if not fts_version:
            conn.close()
            raise click.BadParameter(
                'Your SQLite version does not support any variant of FTS'
            )
        # Check that columns make sense
        for table, df in created_tables.items():
            for fts_column in fts:
                if fts_column not in df.columns:
                    raise click.BadParameter(
                        'FTS column "{}" does not exist'.format(fts_column)
                    )

        generate_and_populate_fts(conn, created_tables.keys(), fts, foreign_keys)

    conn.close()

    if db_existed:
        click.echo('Added {} CSV file{} to {}'.format(
            len(csvs), '' if len(csvs) == 1 else 's', dbname
        ))
    else:
        click.echo('Created {} from {} CSV file{}'.format(
            dbname, len(csvs), '' if len(csvs) == 1 else 's'
        ))
