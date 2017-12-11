# csvs-to-sqlite

[![PyPI](https://img.shields.io/pypi/v/csvs-to-sqlite.svg)](https://pypi.python.org/pypi/csvs-to-sqlite)
[![Travis CI](https://travis-ci.org/simonw/csvs-to-sqlite.svg?branch=master)](https://travis-ci.org/simonw/csvs-to-sqlite)

Convert CSV files into a SQLite database. Browse and publish that SQLite database with [Datasette](https://github.com/simonw/datasette).

Basic usage:

    csvs-to-sqlite myfile.csv mydatabase.db

This will create a new SQLite database called `mydatabase.db` containing a
single table, `myfile`, containing the CSV content.

You can provide multiple CSV files:

    csvs-to-sqlite one.csv two.csv bundle.db

The `bundle.db` database will contain two tables, `one` and `two`.

This means you can use wildcards:

    csvs-to-sqlite ~/Downloads/*.csv my-downloads.db

If you pass a path to one or more directories, the script will recursively
search those directories for CSV files and create tables for each one.

    csvs-to-sqlite ~/path/to/directory all-my-csvs.db

## Handling TSV (tab-separated values)

You can use the `-s` option to specify a different delimiter. If you want
to use a tab character you'll need to apply shell escaping like so:

    csvs-to-sqlite my-file.tsv my-file.db -s $'\t'

## Refactoring columns into separate lookup tables

Let's say you have a CSV file that looks like this:

    county,precinct,office,district,party,candidate,votes
    Clark,1,President,,REP,John R. Kasich,5
    Clark,2,President,,REP,John R. Kasich,0
    Clark,3,President,,REP,John R. Kasich,7

([Real example taken from the Open Elections project](https://github.com/openelections/openelections-data-sd/blob/master/2016/20160607__sd__primary__clark__precinct.csv))

You can now convert selected columns into separate lookup tables using the new
`--extract-`column option (shortname: `-c`) - for example:

    csvs-to-sqlite openelections-data-*/*.csv \
        -c county:County:name \
        -c precinct:Precinct:name \
        -c office -c district -c party -c candidate \
        openelections.db

The format is as follows:

    column_name:optional_table_name:optional_table_value_column_name

If you just specify the column name e.g. `-c office`, the following table will
be created:

    CREATE TABLE "office" (
        "id" INTEGER PRIMARY KEY,
        "value" TEXT
    );

If you specify all three options, e.g. `-c precinct:Precinct:name` the table
will look like this:

    CREATE TABLE "Precinct" (
        "id" INTEGER PRIMARY KEY,
        "name" TEXT
    );

The original tables will be created like this:

    CREATE TABLE "ca__primary__san_francisco__precinct" (
        "county" INTEGER,
        "precinct" INTEGER,
        "office" INTEGER,
        "district" INTEGER,
        "party" INTEGER,
        "candidate" INTEGER,
        "votes" INTEGER,
        FOREIGN KEY (county) REFERENCES County(id),
        FOREIGN KEY (party) REFERENCES party(id),
        FOREIGN KEY (precinct) REFERENCES Precinct(id),
        FOREIGN KEY (office) REFERENCES office(id),
        FOREIGN KEY (candidate) REFERENCES candidate(id)
    );

They will be populated with IDs that reference the new derived tables.

## Installation

    pip install csvs-to-sqlite

## csvs-to-sqlite --help

    Usage: csvs-to-sqlite [OPTIONS] PATHS... DBNAME

      PATHS: paths to individual .csv files or to directories containing .csvs

      DBNAME: name of the SQLite database file to create

    Options:
      -s, --separator TEXT       Field separator in input .csv
      -t, --table-name TEXT      Name of the table to create
      -a, --append-tables        Append to existing tables
      -q, --quoting INTEGER      Control field quoting behavior per csv.QUOTE_*
                                 constants. Use one of QUOTE_MINIMAL (0),
                                 QUOTE_ALL (1), QUOTE_NONNUMERIC (2) or QUOTE_NONE
                                 (3).
      --skip-errors              Skip lines with too many fields instead of
                                 stopping the import
      --replace-tables           Replace tables if they already exist
      -c, --extract-column TEXT  One or more columns to 'extract' into a separate
                                 lookup table. If you pass a simple column name
                                 that column will be replaced with integer foreign
                                 key references to a new table of that name. You
                                 can customize the name of the table like so:

                                     --extract-column state:States:state_name

                                 This will pull unique values from the 'state'
                                 column and use them to populate a new 'States'
                                 table, with an id column primary key and a
                                 state_name column containing the strings from the
                                 original column.
      -f, --fts TEXT             One or more columns to use to populate a full-
                                 text index
      --version                  Show the version and exit.
      --help                     Show this message and exit.
