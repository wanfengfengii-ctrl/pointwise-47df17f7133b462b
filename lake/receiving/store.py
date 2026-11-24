from lake.database import atomic, open_database


def initialize_mirror(db):
    with atomic(db):
        db.execute('''CREATE TABLE IF NOT EXISTS mirror_packages (
            receiving_id INTEGER PRIMARY KEY,
            package_id TEXT NOT NULL UNIQUE,
            lake_id TEXT NOT NULL,
            day TEXT NOT NULL,
            revision INTEGER NOT NULL,
            payload BLOB NOT NULL
        )''')
        db.execute('''CREATE INDEX IF NOT EXISTS mirror_revision_evidence
            ON mirror_packages (lake_id, day, revision)''')
        db.execute('''CREATE TABLE IF NOT EXISTS mirror_projections (
            lake_id TEXT NOT NULL,
            day TEXT NOT NULL,
            partition TEXT NOT NULL,
            checkpoint INTEGER NOT NULL,
            receiving_id INTEGER NOT NULL REFERENCES mirror_packages(receiving_id),
            PRIMARY KEY (lake_id, day, partition, checkpoint)
        )''')


def open_mirror(path):
    db = open_database(path)
    try:
        initialize_mirror(db)
    except BaseException:
        db.close()
        raise
    return db


def receiving_transaction(db):
    return atomic(db)


def mirror_read_transaction(db):
    return atomic(db, write=False)
