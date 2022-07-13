#!/usr/bin/env python

import contextlib
import csv
import getpass
import sys
import typing

import psycopg2
from psycopg2 import extras


class DSN:
    def __init__(self) -> None:
        self.password: typing.Optional[str] = None

    def set_password(self, password: str) -> None:
        self.password = password

    def get(self) -> str:
        if self.password is None:
            password_string = ''
        else:
            password_string = ':{self.password}'
        return f'postgresql://postgres{password_string}@localhost:5433/eating'


dsn = DSN()


def fetch(filename: str) -> list[list[str]]:
    records = []
    with open(filename) as csv_file:
        reader = csv.reader(csv_file, delimiter=';', quotechar='"')
        next(reader)  # headers
        for row in reader:
            if len(row) != 0:
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


@contextlib.contextmanager
def query() -> typing.Generator:
    dsn_string = dsn.get()
    connection = psycopg2.connect(dsn_string)
    cursor = connection.cursor()
    yield cursor
    connection.commit()
    cursor.close()
    connection.close()


def insert(records: list[tuple[str, str]]) -> None:
    with query() as cursor:
        sql = '''
            INSERT INTO products(name, polish_name) VALUES %s;
        '''
        extras.execute_values(cursor, sql, records)


def deduplicate() -> None:
    with query() as cursor:
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
        cursor.execute(sql)


def select(name: str, polish_name: str) -> int:
    with query() as cursor:
        sql = '''
            SELECT id FROM products WHERE name = %s AND polish_name = %s;
        '''
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
    with query() as cursor:
        sql = '''
            INSERT INTO forbidden_eating(a_id, b_id) VALUES %s;
        '''
        extras.execute_values(cursor, sql, ids)


def deduplicate_forbidden():
    with query() as cursor:
        sql = '''
            DELETE FROM forbidden_eating WHERE id IN (
                SELECT DISTINCT(a.id)
                    FROM forbidden_eating a JOIN forbidden_eating b
                    ON a.a_id = b.a_id AND
                       a.b_id = b.b_id AND
                       a.id != b.id
                    WHERE a.id > b.id
            );
        '''
        cursor.execute(sql)


def main(argv: list[str]) -> None:
    if len(argv) != 2:
        print('Usage:\n\tpython main.py $filename')
        exit(1)
    filename = argv[1]

    password = getpass.getpass(prompt='Please enter your password: ')
    dsn.set_password(password)

    records = fetch(filename)
    products_records = transform(records)
    insert(products_records)
    deduplicate()
    insert_forbidden(records)
    deduplicate_forbidden()


if __name__ == '__main__':
    main(sys.argv)
