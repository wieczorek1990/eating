#!/usr/bin/env python

import contextlib
import typing

import psycopg2


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


@contextlib.contextmanager
def query() -> typing.Generator:
    dsn_string = dsn.get()
    connection = psycopg2.connect(dsn_string)
    cursor = connection.cursor()
    yield cursor
    connection.commit()
    cursor.close()
    connection.close()
