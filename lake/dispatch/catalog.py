from dataclasses import dataclass

from lake.database import atomic
from lake.ledger.revisions import OfficialReceipt


@dataclass(frozen=True)
class ReleaseRecord:
    release_id: str
    receipt: OfficialReceipt
    parts: tuple[tuple[str, bytes], ...]

    def part(self, partition):
        for name, payload in self.parts:
            if name == partition:
                return payload
        raise KeyError(partition)


def initialize_catalog(db):
    with atomic(db):
        db.execute('''CREATE TABLE IF NOT EXISTS release_catalog (
            release_id TEXT PRIMARY KEY,
            revision INTEGER NOT NULL UNIQUE REFERENCES official_revisions(revision)
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS release_parts (
            release_id TEXT NOT NULL REFERENCES release_catalog(release_id),
            partition TEXT NOT NULL,
            payload BLOB NOT NULL,
            PRIMARY KEY (release_id, partition)
        )''')


def find_release(db, receipt):
    record = db.execute('''SELECT release_id FROM release_catalog
        JOIN ledger_book ON ledger_book.singleton = 1
        WHERE revision = ? AND lake_id = ? AND day = ?''',
                        (receipt.revision, receipt.lake_id, receipt.day)).fetchone()
    if record is None:
        return None
    return ReleaseRecord(record[0], receipt, read_release_parts(db, record[0]))


def save_release(db, release):
    db.execute('INSERT INTO release_catalog VALUES (?, ?)',
               (release.release_id, release.receipt.revision))
    db.executemany('INSERT INTO release_parts VALUES (?, ?, ?)', (
        (release.release_id, partition, payload) for partition, payload in release.parts
    ))


def read_release_parts(db, release_id):
    return tuple((partition, payload) for partition, payload in db.execute(
        'SELECT partition, payload FROM release_parts WHERE release_id = ? ORDER BY partition',
        (release_id,),
    ))
