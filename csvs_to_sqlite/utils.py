import os
import fnmatch
import pandas as pd


class LoadCsvError(Exception):
    pass


def load_csv(filepath, encodings_to_try=('utf8', 'latin-1')):
    for encoding in encodings_to_try:
        try:
            return pd.DataFrame.from_csv(filepath, encoding=encoding)
        except UnicodeDecodeError:
            continue
        except pd.errors.ParserError, e:
            raise LoadCsvError(e)
    # If we get here, we failed
    raise LoadCsvError('All encodings failed')


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
                    namepath = os.path.join(relpath, os.path.splitext(filename)[0])
                    csvs[namepath] = os.path.join(root, filename)

    return csvs
