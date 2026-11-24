from contextlib import contextmanager
import sqlite3


def open_database(path):
    """Open an idle connection with explicit transaction control."""
    db = sqlite3.connect(path, isolation_level=None)
    try:
        db.execute('PRAGMA foreign_keys = ON')
    except BaseException:
        db.close()
        raise
    return db


@contextmanager
def atomic(db, *, write=True):
    db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
    try:
        yield db
        db.commit()
    except BaseException:
        db.rollback()
        raise
