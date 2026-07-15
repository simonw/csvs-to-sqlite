import csv
import dateparser
import os
import fnmatch
import hashlib
import lru
import math
import re
import sqlite3

from io import StringIO
from urllib.parse import urlparse, uses_relative, uses_netloc, uses_params
from urllib.request import urlopen

import click


class LoadCsvError(Exception):
    pass


# The strings pandas.read_csv() treats as missing values by default -
# preserved so type detection and NULLs match the previous implementation
NA_VALUES = frozenset(
    (
        "",
        "#N/A",
        "#N/A N/A",
        "#NA",
        "-1.#IND",
        "-1.#QNAN",
        "-NaN",
        "-nan",
        "1.#IND",
        "1.#QNAN",
        "<NA>",
        "N/A",
        "NA",
        "NULL",
        "NaN",
        "None",
        "n/a",
        "nan",
        "null",
    )
)

TRUE_VALUES = frozenset(("True", "TRUE", "true"))
FALSE_VALUES = frozenset(("False", "FALSE", "false"))

_int_re = re.compile(r"^\s*[+-]?\d+\s*$")


def isnull(value):
    return value is None or (isinstance(value, float) and math.isnan(value))


class Column:
    "A single column of values, returned by Frame.__getitem__"

    def __init__(self, values):
        self.values = values

    def apply(self, fn):
        return [fn(value) for value in self.values]

    def __iter__(self):
        return iter(self.values)

    def __len__(self):
        return len(self.values)


class Frame:
    """
    A minimal replacement for the pandas DataFrame this tool used to be
    built on: ordered named columns of values, plus the SQLite type each
    column should be stored as.
    """

    def __init__(self, columns, data, types):
        self.columns = list(columns)
        self._data = data  # {column_name: [values]}
        self.types = types  # {column_name: "INTEGER"/"REAL"/"TEXT"}
        self.table_name = None

    @property
    def row_count(self):
        if not self.columns:
            return 0
        return len(self._data[self.columns[0]])

    def __len__(self):
        return self.row_count

    def __getitem__(self, name):
        return Column(self._data[name])

    def __setitem__(self, name, value):
        if isinstance(value, Column):
            value = value.values
        if isinstance(value, (list, tuple)):
            values = list(value)
            assert len(values) == self.row_count
        else:
            values = [value] * self.row_count
        if name not in self.columns:
            self.columns.append(name)
        self._data[name] = values
        self.types[name] = sql_type_for_values(values)

    def drop_columns(self, names):
        for name in names:
            self.columns.remove(name)
            del self._data[name]
            del self.types[name]

    def rename_columns(self, renames):
        self.columns = [renames.get(c, c) for c in self.columns]
        self._data = {renames.get(c, c): v for c, v in self._data.items()}
        self.types = {renames.get(c, c): v for c, v in self.types.items()}

    def to_dicts(self):
        return [
            {column: self._data[column][i] for column in self.columns}
            for i in range(self.row_count)
        ]


def sql_type_for_values(values):
    "Pick a SQLite type for a column of Python values"
    non_null = [v for v in values if not isnull(v)]
    if not values:
        return "TEXT"
    if not non_null:
        # Matches old behavior: an all-NaN float column was detected as
        # containing no non-integer floats, so became INTEGER
        return "INTEGER"
    types = set(type(v) for v in non_null)
    if types <= {bool, int}:
        return "INTEGER"
    if types <= {bool, int, float}:
        if all(float(v).is_integer() for v in non_null):
            # Integer-valued floats (e.g. from a column of ints with some
            # missing values) should be stored as INTEGER
            return "INTEGER"
        return "REAL"
    return "TEXT"


def _parse_float(value):
    # Python's float() is more lenient than the pandas parser was
    if "_" in value:
        raise ValueError("invalid float: {}".format(value))
    return float(value)


def convert_column(raw_values, just_strings=False):
    """
    Given a list of strings-or-None from a CSV column, detect the best
    matching type (mimicking pandas.read_csv inference) and return
    (sqlite_type, converted_values).
    """
    if just_strings:
        return "TEXT", raw_values
    non_null = [v for v in raw_values if v is not None]
    if not raw_values:
        # Zero rows: pandas gave these columns object dtype
        return "TEXT", raw_values
    if not non_null:
        return "INTEGER", raw_values
    if all(v in TRUE_VALUES or v in FALSE_VALUES for v in non_null):
        return (
            "INTEGER",
            [None if v is None else v in TRUE_VALUES for v in raw_values],
        )
    if all(_int_re.match(v) for v in non_null):
        return "INTEGER", [None if v is None else int(v) for v in raw_values]
    try:
        converted = [None if v is None else _parse_float(v) for v in raw_values]
    except ValueError:
        return "TEXT", raw_values
    if all(v is None or v.is_integer() for v in converted):
        # Keep the float values: SQLite's INTEGER column affinity converts
        # 7.0 to 7 on insert, exactly as it did for the old pandas code
        return "INTEGER", converted
    return "REAL", converted


def _dedupe_column_names(names):
    "Rename duplicate columns the way pandas did: a, a.1, a.2, ..."
    result = []
    counts = {}
    for name in names:
        if name in counts:
            counts[name] += 1
            new_name = "{}.{}".format(name, counts[name])
            while new_name in names or new_name in result:
                counts[name] += 1
                new_name = "{}.{}".format(name, counts[name])
            name = new_name
        counts[name] = counts.get(name, 0)
        result.append(name)
    return result


def _read_file_bytes(filepath):
    if _is_url(filepath):
        return urlopen(filepath).read()
    with open(filepath, "rb") as fp:
        return fp.read()


def _parse_csv(text, separator, skip_errors, quoting, usecols, just_strings):
    reader = csv.reader(StringIO(text), delimiter=separator, quoting=quoting)
    try:
        header = next(reader)
    except StopIteration:
        raise LoadCsvError("No columns to parse from file")
    columns = _dedupe_column_names(header)
    num_fields = len(columns)
    keep_indexes = None
    if usecols is not None:
        missing = [c for c in usecols if c not in columns]
        if missing:
            raise LoadCsvError(
                "Usecols do not match columns, columns expected but not "
                "found: {}".format(missing)
            )
        keep_indexes = [i for i, c in enumerate(columns) if c in usecols]
        columns = [columns[i] for i in keep_indexes]
    raw_columns = [[] for _ in columns]
    for row in reader:
        if not row:
            # pandas skipped blank lines by default
            continue
        if len(row) > num_fields:
            if skip_errors:
                continue
            raise LoadCsvError(
                "Error tokenizing data. Expected {} fields in line {}, "
                "saw {}".format(num_fields, reader.line_num, len(row))
            )
        if len(row) < num_fields:
            # pandas filled short rows with missing values
            row = row + [""] * (num_fields - len(row))
        if keep_indexes is not None:
            row = [row[i] for i in keep_indexes]
        for i, cell in enumerate(row):
            if not isinstance(cell, str):
                # csv.QUOTE_NONNUMERIC returns floats for unquoted fields
                cell = repr(cell)
            raw_columns[i].append(None if cell in NA_VALUES else cell)
    data = {}
    types = {}
    for name, raw_values in zip(columns, raw_columns):
        types[name], data[name] = convert_column(raw_values, just_strings)
    return Frame(columns, data, types)


def load_csv(
    filepath,
    separator,
    skip_errors,
    quoting,
    shape,
    encodings_to_try=("utf8", "latin-1"),
    just_strings=False,
):
    usecols = None
    if shape:
        usecols = [defn["csv_name"] for defn in parse_shape(shape)]
    try:
        file_bytes = _read_file_bytes(filepath)
        for encoding in encodings_to_try:
            try:
                text = file_bytes.decode(encoding)
            except UnicodeDecodeError:
                continue
            if text.startswith("\ufeff"):
                # Strip any byte order mark, like pandas did
                text = text[1:]
            return _parse_csv(
                text, separator, skip_errors, quoting, usecols, just_strings
            )
        # If we get here, we failed
        raise LoadCsvError("All encodings failed")
    except LoadCsvError:
        raise
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
            """.format(table_name=self.table_name, value_column=self.value_column)
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
        if isnull(value):
            return None
        # value should be a string
        if not isinstance(value, str):
            if isinstance(value, float):
                value = "{0:g}".format(value)
            else:
                value = str(value)
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
                """.format(table_name=self.table_name, value_column=self.value_column),
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


def _pandas_columns_and_types(df, index=True):
    """
    Column names and SQLite types for a pandas DataFrame or Series.

    This exists so the original test suite - which calls
    get_create_table_sql() with pandas objects - continues to work.
    pandas (and therefore numpy) is a test-only dependency; this function
    never imports either, it just duck-types their objects.
    """
    if hasattr(df, "dtypes"):  # DataFrame
        columns_and_dtypes = list(df.dtypes.items())
        series_for = lambda column: df[column]
    else:  # Series
        columns_and_dtypes = [(df.name, df.dtype)]
        series_for = lambda column: df

    def sql_type(dtype, values):
        kind = dtype.kind
        if kind in "iu":
            return "INTEGER"
        if kind == "f":
            # If every non-NaN value is an integer, switch to INTEGER
            # http://pandas.pydata.org/pandas-docs/stable/gotchas.html#support-for-integer-na
            if all(isnull(v) or float(v).is_integer() for v in values):
                return "INTEGER"
            return "REAL"
        if kind == "b":
            return "INTEGER"
        if kind == "M":
            return "TIMESTAMP"
        return "TEXT"

    columns_and_types = []
    if index and hasattr(df, "index"):
        columns_and_types.append(
            (df.index.name or "index", sql_type(df.index.dtype, df.index))
        )
    for column, dtype in columns_and_dtypes:
        columns_and_types.append((column, sql_type(dtype, series_for(column))))
    return columns_and_types


def get_create_table_sql(
    table_name, frame, index=True, sql_type_overrides=None, primary_keys=None
):
    # Returns (sql, columns)
    sql_type_overrides = sql_type_overrides or {}
    if isinstance(frame, Frame):
        columns_and_types = [(c, frame.types[c]) for c in frame.columns]
    else:
        # A pandas DataFrame or Series (only used by the older tests)
        columns_and_types = _pandas_columns_and_types(frame, index=index)
    # This matches the CREATE TABLE format pandas.DataFrame.to_sql used
    sql = 'CREATE TABLE "{}" (\n{}\n)'.format(
        table_name,
        ",\n  ".join(
            '"{}" {}'.format(name, sql_type_overrides.get(name, type_))
            for name, type_ in columns_and_types
        ),
    )
    columns = [name for name, _ in columns_and_types]
    if primary_keys:
        # Rewrite SQL to add PRIMARY KEY (col1, col2) at end
        assert sql[-1] == ")"
        sql = sql[:-1] + "  ,PRIMARY KEY ({cols})\n)".format(
            cols=", ".join("[{}]".format(col) for col in primary_keys)
        )
    return sql, columns


def to_sql_with_foreign_keys(
    db,
    frame,
    name,
    foreign_keys,
    sql_type_overrides=None,
    primary_keys=None,
    index_fks=False,
):
    create_sql, columns = get_create_table_sql(
        name,
        frame,
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
    db.conn.executescript(create_sql)
    # Now that we have created the table, insert the rows:
    insert_frame(db, frame, name)


def insert_frame(db, frame, name):
    "Insert a Frame's rows into the named table using sqlite-utils"
    rows = frame.to_dicts()
    if rows:
        db[name].insert_all(rows, batch_size=100)


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
        # Is this a rename?
        if ":" in defn:
            csv_name, db_name = defn.split(":", 1)
        else:
            csv_name, db_name = defn, defn
        defns.append(
            {"csv_name": csv_name, "db_name": db_name, "type_override": type_override}
        )
    return defns


def apply_shape(frame, shape):
    # Shape is format 'county:Cty,votes:Vts(REAL)'
    # Applies changes in place, returns type overrides for CREATE TABLE
    if not shape:
        return None
    defns = parse_shape(shape)
    # Drop any columns we don't want
    cols_to_keep = [d["csv_name"] for d in defns]
    cols_to_drop = [c for c in frame.columns if c not in cols_to_keep]
    if cols_to_drop:
        frame.drop_columns(cols_to_drop)
    # Apply column renames
    renames = {
        d["csv_name"]: d["db_name"] for d in defns if d["csv_name"] != d["db_name"]
    }
    if renames:
        frame.rename_columns(renames)
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


def apply_dates_and_datetimes(frame, date_cols, datetime_cols, datetime_formats):
    def parse_datetime(datestring, force_date=False):
        if isnull(datestring):
            return datestring
        dt = dateparser.parse(datestring, date_formats=datetime_formats)
        if force_date:
            return dt.date().isoformat()
        else:
            return dt.isoformat()

    for date_col in date_cols:
        frame[date_col] = frame[date_col].apply(
            lambda s: parse_datetime(s, force_date=True)
        )
    for datetime_col in datetime_cols:
        frame[datetime_col] = frame[datetime_col].apply(parse_datetime)
