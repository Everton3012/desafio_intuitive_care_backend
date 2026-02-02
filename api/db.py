import os
import psycopg

from contextlib import contextmanager
from psycopg.rows import dict_row


def _get_env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Variável de ambiente ausente: {name}")
    return str(v).strip().strip('"').strip("'")


@contextmanager
def get_conn():
    host = _get_env("DB_HOST")
    port = _get_env("DB_PORT")
    dbname = _get_env("DB_NAME")
    user = _get_env("DB_USER")
    password = _get_env("DB_PASSWORD")

    conn = psycopg.connect(
        f"host={host} port={port} dbname={dbname} user={user} password={password}",
        row_factory=dict_row,
        options="-c client_encoding=UTF8",
    )
    
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_cursor(conn):
    """Context manager para cursor"""
    with conn.cursor() as cursor:
        yield cursor