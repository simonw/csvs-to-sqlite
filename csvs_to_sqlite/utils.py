import dateparser
import os
import fnmatch
import hashlib
import lru
import pandas as pd
import numpy as np
import re
import sqlite3
import chardet
from pathlib import Path
from urllib.parse import urlparse, uses_relative, uses_netloc, uses_params
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
    dtype = str if just_strings else None
    usecols = [defn["csv_name"] for defn in parse_shape(shape)] if shape else None
    
    with open(filepath, 'rb') as f:
        result = chardet.detect(f.read(1024))
        encoding = result['encoding'] or encodings_to_try[0]
    
    try:
        return pd.read_csv(
            filepath,
            sep=separator,
            quoting=quoting,
            on_bad_lines="skip" if skip_errors else "error",
            low_memory=False,
            encoding=encoding,
            usecols=usecols,
            dtype=dtype,
        )
    except UnicodeDecodeError:
        for fallback in encodings_to_try:
            if fallback != encoding:
                try:
                    return pd.read_csv(
                        filepath,
                        sep=separator,
                        quoting=quoting,
                        on_bad_lines="skip" if skip_errors else "error",
                        low_memory=False,
                        encoding=fallback,
                        usecols=usecols,
                        dtype=dtype,
                    )
                except UnicodeDecodeError:
                    continue
        raise LoadCsvError("All encodings failed")
    except Exception as e:
        raise LoadCsvError(e)

def csvs_from_paths(paths):
    csvs = {}
    
    def add_item(filepath, full_path=None):
        name = Path(filepath).stem
        i = 1
        while name in csvs:
            name = f"{Path(filepath).stem}-{i}"
            i += 1
        csvs[name] = full_path or filepath

    for path in paths:
        path = Path(path)
        if _is_url(path):
            add_item(Path(urlparse(str(path)).path).name, str(path))
        elif path.is_file():
            add_item(path)
        elif path.is_dir():
            for csv_file in path.rglob("*.csv"):
                relpath = csv_file.relative_to(path).parent
                namepath = str(relpath / csv_file.stem).replace(os.sep, "/")
                csvs[namepath] = str(csv_file)
    
    return csvs

def _is_url(possible_url):
    valid_schemes = set(uses_relative + uses_netloc + uses_params)
    valid_schemes.discard("")
    try:
        return urlparse(str(possible_url)).scheme in valid_schemes
    except:
        return False

class PathOrURL(click.Path):
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
        super().__init__(
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
            return super().convert(value, param, ctx)

class LookupTable:
    def __init__(self, conn, table_name, value_column, index_fts):
        self.conn = conn
        self.table_name = table_name
        self.value_column = value_column
        self.fts_table_name = f"{table_name}_{value_column}_fts"
        self.index_fts = index_fts
        self.cache = lru.LRUCacheDict(max_size=1000)
        self.ensure_table_exists()

    def ensure_table_exists(self):
        with self.conn:
            if not self.conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                AND name=?
            """,
                (self.table_name,),
            ).fetchall():
                create_sql = f"""
                    CREATE TABLE "{self.table_name}" (
                        "id" INTEGER PRIMARY KEY,
                        "{self.value_column}" TEXT
                    );
                """
                self.conn.execute(create_sql)
                if self.index_fts:
                    self.conn.execute(
                        f"""
                        CREATE VIRTUAL TABLE "{self.fts_table_name}"
                        USING {best_fts_version()} ({self.value_column}, content="{self.table_name}");
                    """
                    )

    def __repr__(self):
        with self.conn:
            return f"<{self.table_name}: {self.conn.execute(f'select count(*) from \"{self.table_name}\"').fetchone()[0]} rows>"

    def id_for_value(self, value):
        if pd.isnull(value):
            return None
        value = str(value)

        try:
            return self.cache[value]
        except KeyError:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute(
                    f'INSERT OR IGNORE INTO "{self.table_name}" ("{self.value_column}") VALUES (?)',
                    (value,)
                )
                cursor.execute(
                    f'SELECT id FROM "{self.table_name}" WHERE "{self.value_column}"=?',
                    (value,)
                )
                id = cursor.fetchone()[0]
                
                if self.index_fts and cursor.rowcount > 0:
                    cursor.execute(
                        f'INSERT INTO "{self.fts_table_name}" (rowid, "{self.value_column}") VALUES (?, ?)',
                        (id, value)
                    )
                
                self.cache[value] = id
                return id

def refactor_dataframes(conn, dataframes, foreign_keys, index_fts):
    lookup_tables = {}
    for column, (table_name, value_column) in foreign_keys.items():
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
    with conn:
        return conn.execute(
            """
            select count(*) from sqlite_master
            where type="table" and name=?
        """,
            [table],
        ).fetchone()[0]

def drop_table(conn, table):
    with conn:
        conn.execute(f"DROP TABLE [{table}]")

def get_create_table_sql(
    table_name, df, index=True, sql_type_overrides=None, primary_keys=None
):
    sql_type_overrides = sql_type_overrides or {}
    columns = []
    dtype_map = {
        np.dtype('int64'): 'INTEGER',
        np.dtype('float64'): 'REAL',
        np.dtype('object'): 'TEXT',
        np.dtype('bool'): 'INTEGER',
    }
    
    for col, dtype in df.dtypes.items():
        if col in sql_type_overrides:
            sql_type = sql_type_overrides[col]
        elif dtype == np.float64 and df[col].dropna().apply(float.is_integer).all():
            sql_type = 'INTEGER'
        else:
            sql_type = dtype_map.get(dtype, 'TEXT')
        columns.append(f'"{col}" {sql_type}')
    
    create_sql = f'CREATE TABLE "{table_name}" (\n  ' + ',\n  '.join(columns)
    if primary_keys:
        create_sql += f',\n  PRIMARY KEY ({", ".join(f"[{col}]" for col in primary_keys)})'
    create_sql += '\n)'
    
    return create_sql, df.columns.tolist()

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
                f'FOREIGN KEY ("{column}") REFERENCES [{table}](id)'
            )
            if index_fks:
                index_bits.append(
                    f'CREATE INDEX ["{name}_{column}"] ON [{name}]("{column}");'
                )

    foreign_key_sql = ",\n    ".join(foreign_key_bits)
    if foreign_key_sql:
        create_sql = f"{create_sql.strip().rstrip(')')},\n{foreign_key_sql});"
    if index_bits:
        create_sql += "\n" + "\n".join(index_bits)
    
    with conn:
        conn.executescript(create_sql)
        df.to_sql(name, conn, if_exists="append", index=False)

def best_fts_version():
    with sqlite3.connect(":memory:") as conn:
        for fts in ("FTS5", "FTS4", "FTS3"):
            try:
                conn.execute(f"CREATE VIRTUAL TABLE v USING {fts} (t);")
                return fts
            except sqlite3.OperationalError:
                continue
    return None

def generate_and_populate_fts(conn, created_tables, cols, foreign_keys):
    fts_version = best_fts_version()
    sql = []
    fts_cols = ", ".join(f'"{c}"' for c in cols)
    for table in created_tables:
        sql.append(
            f'CREATE VIRTUAL TABLE "{table}_fts" USING {fts_version} ({fts_cols}, content="{table}")'
        )
        if not foreign_keys:
            select = f"SELECT rowid, {fts_cols} FROM [{table}]"
        else:
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
                        alias = f"table_alias_{hashlib.md5(other_table.encode('utf8')).hexdigest()}_{seen_count}"
                    select_cols.append(f'[{alias or other_table}]."{label_column}"')
                    joins.append(
                        f'left join [{other_table}] {alias} on [{table}]."{col}" = [{alias or other_table}].id'
                    )
                else:
                    select_cols.append(f'"{col}"')
            select = f"SELECT [{table}].rowid, {', '.join(select_cols)} FROM [{table}] {' '.join(joins)}"
        sql.append(
            f'INSERT INTO "{table}_fts" (rowid, {fts_cols}) {select}'
        )
    
    with conn:
        conn.executescript(";\n".join(sql))

type_re = re.compile(r"\((real|integer|text|blob|numeric)\)$", re.I)

def parse_shape(shape):
    defs = [b.strip() for b in shape.split(",")]
    defns = []
    for defn in defs:
        type_override = None
        m = type_re.search(defn)
        if m:
            type_override = m.group(1)
            defn = type_re.sub("", defn)
        csv_name, db_name = defn.split(":", 1) if ":" in defn else (defn, defn)
        defns.append(
            {"csv_name": csv_name, "db_name": db_name, "type_override": type_override}
        )
    return defns

def apply_shape(df, shape):
    if not shape:
        return None
    defns = parse_shape(shape)
    cols_to_keep = [d["csv_name"] for d in defns]
    cols_to_drop = [c for c in df.columns if c not in cols_to_keep]
    if cols_to_drop:
        df.drop(cols_to_drop, axis=1, inplace=True)
    renames = {
        d["csv_name"]: d["db_name"] for d in defns if d["csv_name"] != d["db_name"]
    }
    if renames:
        df.rename(columns=renames, inplace=True)
    return {d["db_name"]: d["type_override"] for d in defns if d["type_override"]}

def add_index(conn, table_name, index):
    columns_to_index = [b.strip() for b in index.split(",")]
    with conn:
        cursor = conn.execute(f"select * from [{table_name}] limit 0")
        columns = [r[0] for r in cursor.description]
        if all(c in columns for c in columns_to_index):
            sql = f'CREATE INDEX ["{table_name}_{"_".join(columns_to_index)}"] ON [{table_name}]("{", ".join(columns_to_index)}");'
            conn.execute(sql)

def apply_dates_and_datetimes(df, date_cols, datetime_cols, datetime_formats):
    def parse_datetime(datestring, force_date=False):
        if pd.isnull(datestring):
            return datestring
        dt = dateparser.parse(datestring, date_formats=datetime_formats)
        return dt.date().isoformat() if force_date else dt.isoformat()

    for date_col in date_cols:
        df[date_col] = df[date_col].apply(lambda s: parse_datetime(s, force_date=True))
    for datetime_col in datetime_cols:
        df[datetime_col] = df[datetime_col].apply(parse_datetime)