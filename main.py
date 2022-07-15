#!/usr/bin/env python

import csv
import getpass
import sys

from psycopg2 import extras

import common
import constants
import psy


def fetch(filename: str) -> list[list[str]]:
    records = []
    with open(filename) as csv_file:
        reader = csv.reader(
            csv_file,
            delimiter=constants.DELIMITER,
            quotechar=constants.QUOTECHAR,
        )
        next(reader)  # skip headers
        for row in reader:
            if len(row) == 0:
                continue
            records.append(row)
    return records


def transform(records: list[list[str]]) -> list[tuple[str, str]]:
    records_set = set()
    for a_name, a_polish_name, b_name, b_polish_name in records:
        a_record = (a_name, a_polish_name)
        b_record = (b_name, b_polish_name)
        records_set.add(a_record)
        records_set.add(b_record)
    return list(records_set)


def insert(records: list[tuple[str, str]]) -> None:
    sql = '''
        INSERT INTO products(name, polish_name) VALUES %s;
    '''
    with psy.query() as cursor:
        extras.execute_values(cursor, sql, records)


def deduplicate() -> None:
    sql = '''
        DELETE FROM products WHERE id IN (
            SELECT a.id
            FROM products a JOIN products b
            ON a.name = b.name AND
               a.polish_name = b.polish_name AND
               a.id != b.id
            WHERE a.id > b.id
        );
    '''
    with psy.query() as cursor:
        cursor.execute(sql)


def select(name: str, polish_name: str) -> int:
    sql = '''
        SELECT id FROM products WHERE name = %s AND polish_name = %s;
    '''
    with psy.query() as cursor:
        cursor.execute(sql, (name, polish_name))
        result = cursor.fetchone()
        if result is not None:
            result = result[0]
        return result


def insert_forbidden(records: list[list[str]]) -> None:
    ids = []
    for a_name, a_polish_name, b_name, b_polish_name in records:
        a_id = select(a_name, a_polish_name)
        b_id = select(b_name, b_polish_name)
        ids.append((a_id, b_id))

    sql = '''
        INSERT INTO forbidden_eating(a_id, b_id) VALUES %s;
    '''
    with psy.query() as cursor:
        extras.execute_values(cursor, sql, ids)


def main(argv: list[str]) -> None:
    if len(argv) != 2:
        print('Usage:\n\tpython main.py $filename')
        exit(1)
    filename = argv[1]

    password = getpass.getpass(prompt='Please enter your password: ')
    psy.dsn.set_password(password)

    records = fetch(filename)
    products_records = transform(records)
    insert(products_records)
    deduplicate()
    insert_forbidden(records)
    common.deduplicate_forbidden()


if __name__ == '__main__':
    main(sys.argv)
