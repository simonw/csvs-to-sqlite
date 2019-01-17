Changelog
=========

0.9 (2019-01-16)
----------------
- Support for loading CSVs directly from URLs, thanks @betatim - #38
- New -pk/--primary-key options, closes #22
- Create FTS index for extracted column values
- Added --no-fulltext-fks option, closes #32
- Now using black for code formatting
- Bumped versions of dependencies

0.8.1 (2018-04-24)
------------------
- Updated README and CHANGELOG, tweaked --help output

0.8 (2018-04-24)
----------------
- `-d` and `-df` options for specifying date/datetime columns, closes #33
- Maintain lookup tables in SQLite, refs #17
- `--index` option to specify which columns to index, closes #24
- Test confirming `--shape` and `--filename-column` and `-c` work together #25
- Use usecols when loading CSV if shape specified
- `--filename-column` is now compatible with `--shape`, closes #10
- `--no-index-fks` option

  By default, csvs-to-sqlite creates an index for every foreign key column that is
  added using the `--extract-column` option.

  For large tables, this can dramatically increase the size of the resulting
  database file on disk. The new `--no-index-fks` option allows you to disable
  this feature to save on file size.

  Refs #24 which will allow you to explicitly list which columns SHOULD have
  an index created.
- Added `--filename-column` option, refs #10
- Fixes for Python 2, refs #25
- Implemented new `--shape` option - refs #25
- `--table` option for specifying table to write to, refs #10
- Updated README to cover `--skip-errors`, refs #20
- Add `--skip-errors` option (#20) [Jani Monoses]
- Less verbosity (#19) [Jani Monoses]

  Only log `extract_columns` info when that option is passed.
- Add option for field quoting behaviour (#15) [Jani Monoses]


0.7 (2017-11-25)
----------------
- Add -s option to specify input field separator (#13) [Jani Monoses]


0.6.1 (2017-11-24)
------------------
- -f and -c now work for single table multiple columns.

  Fixes #12

0.6 (2017-11-24)
----------------
- `--fts` and `--extract-column` now cooperate.

  If you extract a column and then specify that same column in the `--fts` list,
  `csvs-to-sqlite` now uses the original value of that column in the index.

  Example using CSV from https://data.sfgov.org/City-Infrastructure/Street-Tree-List/tkzw-k3nq

      csvs-to-sqlite Street_Tree_List.csv trees-fts.db \
          -c qLegalStatus -c qSpecies -c qSiteInfo \
          -c PlantType -c qCaretaker -c qCareAssistant \
          -f qLegalStatus -f qSpecies -f qAddress \
          -f qSiteInfo -f PlantType -f qCaretaker \
          -f qCareAssistant -f PermitNotes

  Closes #9
- Added `--fts` option for setting up SQLite full-text search.

  The `--fts` option will create a corresponding SQLite FTS virtual table, using
  the best available version of the FTS module.

  https://sqlite.org/fts5.html
  https://www.sqlite.org/fts3.html

  Usage:

      csvs-to-sqlite my-csv.csv output.db -f column1 -f column2

  Example generated with this option: https://sf-trees-search.now.sh/

  Example search: https://sf-trees-search.now.sh/sf-trees-search-a899b92?sql=select+*+from+Street_Tree_List+where+rowid+in+%28select+rowid+from+Street_Tree_List_fts+where+Street_Tree_List_fts+match+%27grove+london+dpw%27%29%0D%0A

  Will be used in https://github.com/simonw/datasette/issues/131
- Handle column names with spaces in them.
- Added `csvs-to-sqlite --version` option.

  Using http://click.pocoo.org/5/api/#click.version_option


0.5 (2017-11-19)
----------------
- Release 0.5.
- Foreign key extraction for mix of integer and NaN now works.

  Similar issue to a8ab5248f4a - when we extracted a column that included a
  mixture of both integers and NaNs things went a bit weird.
- Added test for column extraction.
- Fixed bug with accidentally hard-coded column.


0.4 (2017-11-19)
----------------
- Release 0.4.
- Automatically deploy tags as PyPI releases.

  https://docs.travis-ci.com/user/deployment/pypi/
- Fixed tests for Python 2.
- Ensure columns of ints + NaNs map to SQLite INTEGER.

  Pandas does a good job of figuring out which SQLite column types should be
  used for a DataFrame - with one exception: due to a limitation of NumPy it
  treats columns containing a mixture of integers and NaN (blank values) as
  being of type float64, which means they end up as REAL columns in SQLite.

  http://pandas.pydata.org/pandas-docs/stable/gotchas.html#support-for-integer-na

  To fix this, we now check to see if a float64 column actually consists solely
  of NaN and integer-valued floats (checked using v.is_integer() in Python). If
  that is the case, we over-ride the column type to be INTEGER instead.
- Use miniconda to speed up Travis CI builds (#8)

  Using Travis CI configuration code copied from https://github.com/EducationalTestingService/skll/blob/87b071743ba7cf0b1063c7265005d43b172b5d91/.travis.yml

  Which is itself an updated version of the pattern described in http://dan-blanchard.roughdraft.io/7045057-quicker-travis-builds-that-rely-on-numpy-and-scipy-using-miniconda

  I had to switch to running `pytest` directly, because `python setup.py test` was still trying to install a pandas package that involved compiling everything from scratch (which is why Travis CI builds were taking around 15 minutes).
- Don't include an `index` column - rely on SQLite rowid instead.


0.3 (2017-11-17)
----------------
- Added `--extract-column` to README.

  Also updated the `--help` output and added a Travis CI badge.
- Configure Travis CI.

  Also made it so `python setup.py test` runs the tests.
- Mechanism for converting columns into separate tables.

  Let's say you have a CSV file that looks like this:

      county,precinct,office,district,party,candidate,votes
      Clark,1,President,,REP,John R. Kasich,5
      Clark,2,President,,REP,John R. Kasich,0
      Clark,3,President,,REP,John R. Kasich,7

  (Real example from https://github.com/openelections/openelections-data-sd/blob/master/2016/20160607__sd__primary__clark__precinct.csv )

  You can now convert selected columns into separate lookup tables using the new
  `--extract-column` option (shortname: `-c`) - for example:

      csvs-to-sqlite openelections-data-*/*.csv \
          -c county:County:name \
          -c precinct:Precinct:name \
          -c office -c district -c party -c candidate \
          openelections.db

  The format is as follows:

      column_name:optional_table_name:optional_table_value_column_name

  If you just specify the column name e.g. `-c office`, the following table will
  be created:

      CREATE TABLE "party" (
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

  Closes #2
- Can now add new tables to existing database.

  And the new `--replace-tables` option allows you to tell it to replace existing
  tables rather than quitting with an error.

  Closes #1
- Fixed compatibility with Python 3.
- Badge links to PyPI.
- Create LICENSE.
- Create README.md.
- Initial release.
