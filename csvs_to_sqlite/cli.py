from __future__ import absolute_import

import click
from .utils import (
    LoadCsvError,
    LookupTable,
    PathOrURL,
    add_index,
    apply_dates_and_datetimes,
    apply_shape,
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
@click.argument("paths", type=PathOrURL(exists=True), nargs=-1, required=True)
@click.argument("dbname", nargs=1)
@click.option("--separator", "-s", default=",", help="Field separator in input .csv")
@click.option(
    "--quoting",
    "-q",
    default=0,
    help="Control field quoting behavior per csv.QUOTE_* constants. Use one of QUOTE_MINIMAL (0), QUOTE_ALL (1), QUOTE_NONNUMERIC (2) or QUOTE_NONE (3).",
)
@click.option(
    "--skip-errors",
    is_flag=True,
    help="Skip lines with too many fields instead of stopping the import",
)
@click.option(
    "--replace-tables", is_flag=True, help="Replace tables if they already exist"
)
@click.option(
    "--table", "-t", help="Table to use (instead of using CSV filename)", default=None
)
@click.option(
    "--extract-column",
    "-c",
    multiple=True,
    help=(
        "One or more columns to 'extract' into a separate lookup table. "
        "If you pass a simple column name that column will be replaced "
        "with integer foreign key references to a new table of that "
        "name. You can customize the name of the table like so:\n"
        "    state:States:state_name\n\n"
        "This will pull unique values from the 'state' column and use "
        "them to populate a new 'States' table, with an id column "
        "primary key and a state_name column containing the strings "
        "from the original column."
    ),
)
@click.option(
    "--date",
    "-d",
    multiple=True,
    help=("One or more columns to parse into ISO formatted dates"),
)
@click.option(
    "--datetime",
    "-dt",
    multiple=True,
    help=("One or more columns to parse into ISO formatted datetimes"),
)
@click.option(
    "--datetime-format",
    "-df",
    multiple=True,
    help=("One or more custom date format strings to try when parsing dates/datetimes"),
)
@click.option(
    "--primary-key",
    "-pk",
    multiple=True,
    help=("One or more columns to use as the primary key"),
)
@click.option(
    "--fts",
    "-f",
    multiple=True,
    help=("One or more columns to use to populate a full-text index"),
)
@click.option(
    "--index",
    "-i",
    multiple=True,
    help=("Add index on this column (or a compound index with -i col1,col2)"),
)
@click.option(
    "--shape",
    help="Custom shape for the DB table - format is csvcol:dbcol(TYPE),...",
    default=None,
)
@click.option(
    "--filename-column",
    help="Add a column with this name and populate with CSV file name",
    default=None,
)
@click.option(
    "fixed_columns",
    "--fixed-column",
    type=(str, str),
    multiple=True,
    help="Populate column with a fixed string",
    default=None,
)
@click.option(
    "fixed_columns_int",
    "--fixed-column-int",
    type=(str, int),
    multiple=True,
    help="Populate column with a fixed integer",
    default=None,
)
@click.option(
    "fixed_columns_float",
    "--fixed-column-float",
    type=(str, float),
    multiple=True,
    help="Populate column with a fixed float",
    default=None,
)
@click.option(
    "--no-index-fks",
    "no_index_fks",
    is_flag=True,
    help="Skip adding index to foreign key columns created using --extract-column (default is to add them)",
)
@click.option(
    "--no-fulltext-fks",
    "no_fulltext_fks",
    is_flag=True,
    help="Skip adding full-text index on values extracted using --extract-column (default is to add them)",
)
@click.option(
    "--just-strings",
    is_flag=True,
    help="Import all columns as text strings by default (and, if specified, still obey --shape, --date/datetime, and --datetime-format)",
)
@click.version_option()
def cli(
    paths,
    dbname,
    separator,
    quoting,
    skip_errors,
    replace_tables,
    table,
    extract_column,
    date,
    datetime,
    datetime_format,
    primary_key,
    fts,
    index,
    shape,
    filename_column,
    fixed_columns,
    fixed_columns_int,
    fixed_columns_float,
    no_index_fks,
    no_fulltext_fks,
    just_strings,
):
    """
    PATHS: paths to individual .csv files or to directories containing .csvs

    DBNAME: name of the SQLite database file to create
    """
    # make plural for more readable code:
    extract_columns = extract_column
    del extract_column

    if extract_columns:
        click.echo("extract_columns={}".format(extract_columns))
    if dbname.endswith(".csv"):
        raise click.BadParameter("dbname must not end with .csv")
    if "." not in dbname:
        dbname += ".db"

    db_existed = os.path.exists(dbname)

    conn = sqlite3.connect(dbname)

    dataframes = []
    csvs = csvs_from_paths(paths)
    sql_type_overrides = None
    for name, path in csvs.items():
        try:
            df = load_csv(
                path, separator, skip_errors, quoting, shape, just_strings=just_strings
            )
            df.table_name = table or name
            if filename_column:
                df[filename_column] = name
                if shape:
                    shape += ",{}".format(filename_column)
            if fixed_columns:
                for colname, value in fixed_columns:
                    df[colname] = value
                    if shape:
                        shape += ",{}".format(colname)
            if fixed_columns_int:
                for colname, value in fixed_columns_int:
                    df[colname] = value
                    if shape:
                        shape += ",{}".format(colname)
            if fixed_columns_float:
                for colname, value in fixed_columns_float:
                    df[colname] = value
                    if shape:
                        shape += ",{}".format(colname)
            sql_type_overrides = apply_shape(df, shape)
            apply_dates_and_datetimes(df, date, datetime, datetime_format)
            dataframes.append(df)
        except LoadCsvError as e:
            click.echo("Could not load {}: {}".format(path, e), err=True)

    click.echo("Loaded {} dataframes".format(len(dataframes)))

    # Use extract_columns to build a column:(table,label) dictionary
    foreign_keys = {}
    for col in extract_columns:
        bits = col.split(":")
        if len(bits) == 3:
            foreign_keys[bits[0]] = (bits[1], bits[2])
        elif len(bits) == 2:
            foreign_keys[bits[0]] = (bits[1], "value")
        else:
            foreign_keys[bits[0]] = (bits[0], "value")

    # Now we have loaded the dataframes, we can refactor them
    created_tables = {}
    refactored = refactor_dataframes(
        conn, dataframes, foreign_keys, not no_fulltext_fks
    )
    for df in refactored:
        # This is a bit trickier because we need to
        # create the table with extra SQL for foreign keys
        if replace_tables and table_exists(conn, df.table_name):
            drop_table(conn, df.table_name)
        if table_exists(conn, df.table_name):
            df.to_sql(df.table_name, conn, if_exists="append", index=False)
        else:
            to_sql_with_foreign_keys(
                conn,
                df,
                df.table_name,
                foreign_keys,
                sql_type_overrides,
                primary_keys=primary_key,
                index_fks=not no_index_fks,
            )
            created_tables[df.table_name] = df
        if index:
            for index_defn in index:
                add_index(conn, df.table_name, index_defn)

    # Create FTS tables
    if fts:
        fts_version = best_fts_version()
        if not fts_version:
            conn.close()
            raise click.BadParameter(
                "Your SQLite version does not support any variant of FTS"
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
        click.echo(
            "Added {} CSV file{} to {}".format(
                len(csvs), "" if len(csvs) == 1 else "s", dbname
            )
        )
    else:
        click.echo(
            "Created {} from {} CSV file{}".format(
                dbname, len(csvs), "" if len(csvs) == 1 else "s"
            )
        )
