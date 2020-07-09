import dateparser
import os
import fnmatch
import hashlib
import lru
import pandas as pd
import numpy as np
import re
import six
import sqlite3

from six.moves.urllib.parse import urlparse
from six.moves.urllib.parse import uses_relative, uses_netloc, uses_params

import click


class LoadCsvError(Exception):
    pass


def load_csv(
    filepath,
    separator,
    skip_errors,
    quoting,
    shape,
    encodings_to_try=("utf8", "latin-1"),
    just_strings=False,
):
    dtype = str if just_strings is True else None
    usecols = None
    if shape:
        usecols = [defn["csv_name"] for defn in parse_shape(shape)]
    try:
        for encoding in encodings_to_try:
            try:
                return pd.read_csv(
                    filepath,
                    sep=separator,
                    quoting=quoting,
                    error_bad_lines=not skip_errors,
                    low_memory=True,
                    encoding=encoding,
                    usecols=usecols,
                    dtype=dtype,
                )
            except UnicodeDecodeError:
                continue
            except pd.errors.ParserError as e:
                raise LoadCsvError(e)
        # If we get here, we failed
        raise LoadCsvError("All encodings failed")
    except Exception as e:
        raise LoadCsvError(e)


def csvs_from_paths(paths):
    csvs = {}

    def add_item(filepath, full_path=None):
        name = os.path.splitext(os.path.basename(filepath))[0]
        if name in csvs:
            i = 1
            while True:
                name_plus_suffix = "{}-{}".format(name, i)
                if name_plus_suffix not in csvs:
                    name = name_plus_suffix
                    break
                else:
                    i += 1
        if full_path is None:
            csvs[name] = filepath
        else:
            csvs[name] = full_path

    for path in paths:
        if os.path.isfile(path):
            add_item(path)
        elif _is_url(path):
            add_item(urlparse(path).path, path)
        elif os.path.isdir(path):
            # Recursively seek out ALL csvs in directory
            for root, dirnames, filenames in os.walk(path):
                for filename in fnmatch.filter(filenames, "*.csv"):
                    relpath = os.path.relpath(root, path)
                    namepath = os.path.join(relpath, os.path.splitext(filename)[0])
                    csvs[namepath] = os.path.join(root, filename)

    return csvs


def _is_url(possible_url):
    valid_schemes = set(uses_relative + uses_netloc + uses_params)
    valid_schemes.discard("")

    try:
        return urlparse(possible_url).scheme in valid_schemes
    except:
        return False


class PathOrURL(click.Path):
    """The PathOrURL type handles paths or URLs.

    If the argument can be parsed as a URL, it will be treated as one.
    Otherwise PathorURL behaves like click.Path.
    """

    def __init__(
        self,
        exists=False,
        file_okay=True,
        dir_okay=True,
        writable=False,
        readable=True,
        resolve_path=False,
        allow_dash=False,
        path_type=None,
    ):
        super(PathOrURL, self).__init__(
            exists=exists,
            file_okay=file_okay,
            dir_okay=dir_okay,
            writable=writable,
            readable=readable,
            resolve_path=resolve_path,
            allow_dash=allow_dash,
            path_type=path_type,
        )

    def convert(self, value, param, ctx):
        if _is_url(value):
            return self.coerce_path_result(value)
        else:
            return super(PathOrURL, self).convert(value, param, ctx)


class LookupTable:
    def __init__(self, conn, table_name, value_column, index_fts):
        self.conn = conn
        self.table_name = table_name
        self.value_column = value_column
        self.fts_table_name = "{table_name}_{value_column}_fts".format(
            table_name=table_name, value_column=value_column
        )
        self.index_fts = index_fts
        self.cache = lru.LRUCacheDict(max_size=1000)
        self.ensure_table_exists()

    def ensure_table_exists(self):
        if not self.conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table'
            AND name=?
        """,
            (self.table_name,),
        ).fetchall():
            create_sql = """
                CREATE TABLE "{table_name}" (
                    "id" INTEGER PRIMARY KEY,
                    "{value_column}" TEXT
                );
            """.format(
                table_name=self.table_name, value_column=self.value_column
            )
            self.conn.execute(create_sql)
            if self.index_fts:
                # Add a FTS index on the value_column
                self.conn.execute(
                    """
                    CREATE VIRTUAL TABLE "{fts_table_name}"
                    USING {fts_version} ({value_column}, content="{table_name}");
                """.format(
                        fts_version=best_fts_version(),
                        fts_table_name=self.fts_table_name,
                        table_name=self.table_name,
                        value_column=self.value_column,
                    )
                )

    def __repr__(self):
        return "<{}: {} rows>".format(
            self.table_name,
            self.conn.execute(
                'select count(*) from "{}"'.format(self.table_name)
            ).fetchone()[0],
        )

    def id_for_value(self, value):
        if pd.isnull(value):
            return None
        # value should be a string
        if not isinstance(value, six.string_types):
            if isinstance(value, float):
                value = "{0:g}".format(value)
            else:
                value = six.text_type(value)
        try:
            # First try our in-memory cache
            return self.cache[value]
        except KeyError:
            # Next try the database table
            sql = 'SELECT id FROM "{table_name}" WHERE "{value_column}"=?'.format(
                table_name=self.table_name, value_column=self.value_column
            )
            result = self.conn.execute(sql, (value,)).fetchall()
            if result:
                id = result[0][0]
            else:
                # Not in DB! Insert it
                cursor = self.conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO "{table_name}" ("{value_column}") VALUES (?);
                """.format(
                        table_name=self.table_name, value_column=self.value_column
                    ),
                    (value,),
                )
                id = cursor.lastrowid
                if self.index_fts:
                    # And update FTS index
                    sql = """
                        INSERT INTO "{fts_table_name}" (rowid, "{value_column}") VALUES (?, ?);
                    """.format(
                        fts_table_name=self.fts_table_name,
                        value_column=self.value_column,
                    )
                    cursor.execute(sql, (id, value))

            self.cache[value] = id
            return id


def refactor_dataframes(conn, dataframes, foreign_keys, index_fts):
    lookup_tables = {}
    for column, (table_name, value_column) in foreign_keys.items():
        # Now apply this to the dataframes
        for dataframe in dataframes:
            if column in dataframe.columns:
                lookup_table = lookup_tables.get(table_name)
                if lookup_table is None:
                    lookup_table = LookupTable(
                        conn=conn,
                        table_name=table_name,
                        value_column=value_column,
                        index_fts=index_fts,
                    )
                    lookup_tables[table_name] = lookup_table
                dataframe[column] = dataframe[column].apply(lookup_table.id_for_value)
    return dataframes


def table_exists(conn, table):
    return conn.execute(
        """
        select count(*) from sqlite_master
        where type="table" and name=?
    """,
        [table],
    ).fetchone()[0]


def drop_table(conn, table):
    conn.execute("DROP TABLE [{}]".format(table))


def get_create_table_sql(
    table_name, df, index=True, sql_type_overrides=None, primary_keys=None
):
    # Create a temporary table with just the first row
    # We do this in memory because we just want to get the
    # CREATE TABLE statement
    # Returns (sql, columns)
    conn = sqlite3.connect(":memory:")
    # Before calling to_sql we need correct the dtypes that we will be using
    # to pick the right SQL column types. pandas mostly gets this right...
    # except for columns that contain a mixture of integers and Nones. These
    # will be incorrectly detected as being of DB type REAL when we want them
    # to be INTEGER instead.
    # http://pandas.pydata.org/pandas-docs/stable/gotchas.html#support-for-integer-na
    sql_type_overrides = sql_type_overrides or {}
    if isinstance(df, pd.DataFrame):
        columns_and_types = df.dtypes.iteritems()
    elif isinstance(df, pd.Series):
        columns_and_types = [(df.name, df.dtype)]
    for column, dtype in columns_and_types:
        # Are any of these float columns?
        if dtype in (np.float32, np.float64):
            # if every non-NaN value is an integer, switch to int
            if isinstance(df, pd.Series):
                series = df
            else:
                series = df[column]
            num_non_integer_floats = series.map(
                lambda v: not np.isnan(v) and not v.is_integer()
            ).sum()
            if num_non_integer_floats == 0:
                # Everything was NaN or an integer-float - switch type:
                sql_type_overrides[column] = "INTEGER"

    df[:1].to_sql(table_name, conn, index=index, dtype=sql_type_overrides)
    sql = conn.execute(
        "select sql from sqlite_master where name = ?", [table_name]
    ).fetchone()[0]
    columns = [
        row[1] for row in conn.execute("PRAGMA table_info([{}])".format(table_name))
    ]
    if primary_keys:
        # Rewrite SQL to add PRIMARY KEY (col1, col2) at end
        assert sql[-1] == ")"
        sql = sql[:-1] + "  ,PRIMARY KEY ({cols})\n)".format(
            cols=", ".join("[{}]".format(col) for col in primary_keys)
        )
    return sql, columns


def to_sql_with_foreign_keys(
    conn,
    df,
    name,
    foreign_keys,
    sql_type_overrides=None,
    primary_keys=None,
    index_fks=False,
):
    create_sql, columns = get_create_table_sql(
        name,
        df,
        index=False,
        primary_keys=primary_keys,
        sql_type_overrides=sql_type_overrides,
    )
    foreign_key_bits = []
    index_bits = []
    for column, (table, value_column) in foreign_keys.items():
        if column in columns:
            foreign_key_bits.append(
                'FOREIGN KEY ("{}") REFERENCES [{}](id)'.format(column, table)
            )
            if index_fks:
                index_bits.append(
                    # CREATE INDEX indexname ON table(column);
                    'CREATE INDEX ["{}_{}"] ON [{}]("{}");'.format(
                        name, column, name, column
                    )
                )

    foreign_key_sql = ",\n    ".join(foreign_key_bits)
    if foreign_key_sql:
        create_sql = "{},\n{});".format(create_sql.strip().rstrip(")"), foreign_key_sql)
    if index_bits:
        create_sql += "\n" + "\n".join(index_bits)
    conn.executescript(create_sql)
    # Now that we have created the table, insert the rows:
    df.to_sql(df.table_name, conn, if_exists="append", index=False)


def best_fts_version():
    "Discovers the most advanced supported SQLite FTS version"
    conn = sqlite3.connect(":memory:")
    for fts in ("FTS5", "FTS4", "FTS3"):
        try:
            conn.execute("CREATE VIRTUAL TABLE v USING {} (t);".format(fts))
            return fts
        except sqlite3.OperationalError:
            continue
    return None


def generate_and_populate_fts(conn, created_tables, cols, foreign_keys):
    fts_version = best_fts_version()
    sql = []
    fts_cols = ", ".join('"{}"'.format(c) for c in cols)
    for table in created_tables:
        sql.append(
            'CREATE VIRTUAL TABLE "{content_table}_fts" USING {fts_version} ({cols}, content="{content_table}")'.format(
                cols=fts_cols, content_table=table, fts_version=fts_version
            )
        )
        if not foreign_keys:
            # Select is simple:
            select = "SELECT rowid, {cols} FROM [{content_table}]".format(
                cols=fts_cols, content_table=table
            )
        else:
            # Select is complicated:
            # select
            #     county, precinct, office.value, district.value,
            #     party.value, candidate.value, votes
            # from content_table
            #     left join office on content_table.office = office.id
            #     left join district on content_table.district = district.id
            #     left join party on content_table.party = party.id
            #     left join candidate on content_table.candidate = candidate.id
            # order by content_table.rowid
            select_cols = []
            joins = []
            table_seen_count = {}
            for col in cols:
                if col in foreign_keys:
                    other_table, label_column = foreign_keys[col]
                    seen_count = table_seen_count.get(other_table, 0) + 1
                    table_seen_count[other_table] = seen_count
                    alias = ""
                    if seen_count > 1:
                        alias = "table_alias_{}_{}".format(
                            hashlib.md5(other_table.encode("utf8")).hexdigest(),
                            seen_count,
                        )
                    select_cols.append(
                        '[{}]."{}"'.format(alias or other_table, label_column)
                    )
                    joins.append(
                        'left join [{other_table}] {alias} on [{table}]."{column}" = [{alias_or_other_table}].id'.format(
                            other_table=other_table,
                            alias_or_other_table=alias or other_table,
                            alias=alias,
                            table=table,
                            column=col,
                        )
                    )
                else:
                    select_cols.append('"{}"'.format(col))
            select = "SELECT [{content_table}].rowid, {select_cols} FROM [{content_table}] {joins}".format(
                select_cols=", ".join("{}".format(c) for c in select_cols),
                content_table=table,
                joins="\n".join(joins),
            )
        sql.append(
            'INSERT INTO "{content_table}_fts" (rowid, {cols}) {select}'.format(
                cols=fts_cols, content_table=table, select=select
            )
        )
    conn.executescript(";\n".join(sql))


type_re = re.compile(r"\((real|integer|text|blob|numeric)\)$", re.I)


def parse_shape(shape):
    # Shape is format 'county:Cty,votes:Vts(REAL)'
    defs = [b.strip() for b in shape.split(",")]
    defns = []
    for defn in defs:
        # Is there a type defined?
        type_override = None
        m = type_re.search(defn)
        if m:
            type_override = m.group(1)
            defn = type_re.sub("", defn)
            # In Python 2 type_override needs to be a bytestring
            if six.PY2:
                type_override = str(type_override)
        # Is this a rename?
        if ":" in defn:
            csv_name, db_name = defn.split(":", 1)
        else:
            csv_name, db_name = defn, defn
        defns.append(
            {"csv_name": csv_name, "db_name": db_name, "type_override": type_override}
        )
    return defns


def apply_shape(df, shape):
    # Shape is format 'county:Cty,votes:Vts(REAL)'
    # Applies changes in place, returns dtype= arg for to_sql
    if not shape:
        return None
    defns = parse_shape(shape)
    # Drop any columns we don't want
    cols_to_keep = [d["csv_name"] for d in defns]
    cols_to_drop = [c for c in df.columns if c not in cols_to_keep]
    if cols_to_drop:
        df.drop(cols_to_drop, axis=1, inplace=True)
    # Apply column renames
    renames = {
        d["csv_name"]: d["db_name"] for d in defns if d["csv_name"] != d["db_name"]
    }
    if renames:
        df.rename(columns=renames, inplace=True)
    # Return type overrides, if any
    return {d["db_name"]: d["type_override"] for d in defns if d["type_override"]}


def add_index(conn, table_name, index):
    columns_to_index = [b.strip() for b in index.split(",")]
    # Figure out columns in table so we can sanity check this
    cursor = conn.execute("select * from [{}] limit 0".format(table_name))
    columns = [r[0] for r in cursor.description]
    if all([(c in columns) for c in columns_to_index]):
        sql = 'CREATE INDEX ["{}_{}"] ON [{}]("{}");'.format(
            table_name,
            "_".join(columns_to_index),
            table_name,
            '", "'.join(columns_to_index),
        )
        conn.execute(sql)


def apply_dates_and_datetimes(df, date_cols, datetime_cols, datetime_formats):
    def parse_datetime(datestring, force_date=False):
        if pd.isnull(datestring):
            return datestring
        dt = dateparser.parse(datestring, date_formats=datetime_formats)
        if force_date:
            return dt.date().isoformat()
        else:
            return dt.isoformat()

    for date_col in date_cols:
        df[date_col] = df[date_col].apply(lambda s: parse_datetime(s, force_date=True))
    for datetime_col in datetime_cols:
        df[datetime_col] = df[datetime_col].apply(parse_datetime)
