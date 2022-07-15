#!/usr/bin/env python

import csv
import sys
import typing

from psycopg2 import extras

import common
import constants
import psy


def fetch(filename: str, cast: typing.Callable = int) -> list[tuple]:
    ids = []
    with open(filename) as file_descriptor:
        reader = csv.reader(
            file_descriptor,
            delimiter=constants.DELIMITER,
            quotechar=constants.QUOTECHAR,
        )
        for row in reader:
            if len(row) == 0:
                continue
            ids.append(tuple([cast(number) for number in row]))
    return ids



def insert(ids: list[tuple], column_names: list[str]) -> None:
    columns = ', '.join(column_names)
    sql = f'''
        INSERT INTO forbidden_eating({columns}) VALUES %s
    '''
    with psy.query() as cursor:
        extras.execute_values(cursor, sql, ids)


def main(argv: list[str]) -> None:
    if len(argv) < 3:
        print('Usage:\n\tinserter.py csv_file [column_name ...]')
        exit(1)
    filename = argv[1]
    column_names = argv[2:]
    ids = fetch(filename)
    insert(ids, column_names)
    common.deduplicate_forbidden()


if __name__ == '__main__':
    main(sys.argv)
