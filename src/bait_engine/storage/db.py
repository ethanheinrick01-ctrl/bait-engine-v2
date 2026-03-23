from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sqlite3

from bait_engine.storage.schema import ensure_schema

DEFAULT_DB_PATH = Path(".data/bait-engine.db")


def connect(db_path: str | Path | None = None) -> sqlite3.Connection:
    path = Path(db_path or DEFAULT_DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    ensure_schema(conn)
    return conn


@contextmanager
def open_db(db_path: str | Path | None = None):
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()
