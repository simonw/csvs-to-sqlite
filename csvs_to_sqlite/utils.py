import os
import fnmatch
import pandas as pd
import numpy as np
import sqlite3


class LoadCsvError(Exception):
    pass


def load_csv(filepath, encodings_to_try=('utf8', 'latin-1')):
    try:
        for encoding in encodings_to_try:
            try:
                return pd.read_csv(filepath, encoding=encoding)
            except UnicodeDecodeError:
                continue
            except pd.errors.ParserError as e:
                raise LoadCsvError(e)
        # If we get here, we failed
        raise LoadCsvError('All encodings failed')
    except Exception as e:
        raise LoadCsvError(e)


def csvs_from_paths(paths):
    csvs = {}

    def add_file(filepath):
        name = os.path.splitext(os.path.basename(filepath))[0]
        if name in csvs:
            i = 1
            while True:
                name_plus_suffix = '{}-{}'.format(name, i)
                if name_plus_suffix not in csvs:
                    name = name_plus_suffix
                    break
                else:
                    i += 1
        csvs[name] = filepath

    for path in paths:
        if os.path.isfile(path):
            add_file(path)
        elif os.path.isdir(path):
            # Recursively seek out ALL csvs in directory
            for root, dirnames, filenames in os.walk(path):
                for filename in fnmatch.filter(filenames, '*.csv'):
                    relpath = os.path.relpath(root, path)
                    namepath = os.path.join(
                        relpath, os.path.splitext(filename)[0]
                    )
                    csvs[namepath] = os.path.join(root, filename)

    return csvs


class LookupTable:
    # This should probably be a pandas Series or DataFrame
    def __init__(self, table_name, value_column):
        self.table_name = table_name
        self.value_column = value_column
        self.next_id = 1
        self.id_to_value = {}
        self.value_to_id = {}

    def __repr__(self):
        return '<{}: {} rows>'.format(
            self.table_name, len(self.id_to_value)
        )

    def id_for_value(self, value):
        if pd.isnull(value):
            return None
        try:
            return self.value_to_id[value]
        except KeyError:
            id = self.next_id
            self.id_to_value[id] = value
            self.value_to_id[value] = id
            self.next_id += 1
            return id

    def to_sql(self, name, conn):
        create_sql, columns = get_create_table_sql(name, pd.Series(
            self.id_to_value,
            name=self.value_column,
        ), index_label='id')
        # This table does not have a primary key. Let's fix that:
        before, after = create_sql.split('"id" INTEGER', 1)
        create_sql = '{} "id" INTEGER PRIMARY KEY {}'.format(
            before, after,
        )
        conn.executescript(create_sql)
        # Now that we have created the table, insert the rows:
        pd.Series(
            self.id_to_value,
            name=self.value_column,
        ).to_sql(
            name,
            conn,
            if_exists='append',
            index_label='id'
        )


def refactor_dataframes(dataframes, extract_columns):
    lookup_tables = {}
    for extract_column in extract_columns:
        bits = extract_column.split(':')
        column = bits.pop(0)
        if bits:
            table_name = bits.pop(0)
        else:
            table_name = column
        if bits:
            value_column = bits[0]
        else:
            value_column = 'value'
        # Now apply this to the dataframes
        for dataframe in dataframes:
            if column in dataframe.columns:
                lookup_table = lookup_tables.get(table_name)
                if lookup_table is None:
                    lookup_table = LookupTable(
                        table_name=table_name,
                        value_column=value_column,
                    )
                    lookup_tables[table_name] = lookup_table
                dataframe[column] = dataframe[column].apply(
                    lookup_table.id_for_value
                )
    return list(lookup_tables.values()) + dataframes


def table_exists(conn, table):
    return conn.execute('''
        select count(*) from sqlite_master
        where type="table" and name=?
    ''', [table]).fetchone()[0]


def drop_table(conn, table):
    conn.execute('DROP TABLE [{}]'.format(table))


def get_create_table_sql(table_name, df, index=True, **extra_args):
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
    sql_type_overrides = {}
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
                sql_type_overrides[column] = 'INTEGER'

    df[:1].to_sql(table_name, conn, index=index, dtype=sql_type_overrides, **extra_args)
    sql = conn.execute(
        'select sql from sqlite_master where name = ?', [table_name]
    ).fetchone()[0]
    columns = [
        row[1] for row in conn.execute(
            'PRAGMA table_info([{}])'.format(table_name)
        )
    ]
    return sql, columns


def to_sql_with_foreign_keys(conn, df, name, foreign_keys):
    create_sql, columns = get_create_table_sql(name, df, index=False)
    foreign_key_bits = []
    index_bits = []
    for column, table in foreign_keys.items():
        if column in columns:
            foreign_key_bits.append(
                'FOREIGN KEY ({}) REFERENCES {}(id)'.format(
                    column, table
                )
            )
            index_bits.append(
                # CREATE INDEX indexname ON table(column);
                'CREATE INDEX [{}_{}] ON [{}]([{}]);'.format(
                    name, column, name, column
                )
            )

    foreign_key_sql = ',\n    '.join(foreign_key_bits)
    if foreign_key_sql:
        create_sql = '{},\n{});'.format(
            create_sql.strip().rstrip(')'), foreign_key_sql
        )
    if index_bits:
        create_sql += '\n' + '\n'.join(index_bits)
    conn.executescript(create_sql)
    # Now that we have created the table, insert the rows:
    df.to_sql(
        df.table_name,
        conn,
        if_exists='append',
        index=False,
    )
