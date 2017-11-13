from setuptools import setup, find_packages

setup(
    name='csvs_to_sqlite',
    description='Convert CSV files into a SQLite database',
    author='Simon Willison',
    version='0.2',
    license='Apache License, Version 2.0',
    packages=find_packages(),
    install_requires=[
        'click==6.7',
        'pandas==0.20.3',
    ],
    entry_points='''
        [console_scripts]
        csvs-to-sqlite=csvs_to_sqlite.cli:cli
    ''',
    url='https://github.com/simonw/csvs-to-sqlite',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Intended Audience :: End Users/Desktop',
        'Topic :: Database',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
    ],
)
