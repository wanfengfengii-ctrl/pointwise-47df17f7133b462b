from dataclasses import dataclass
import json

from lake.database import atomic
from lake.intake.source_rows import ObservationKey, decode_mm, encode_mm
from lake.intake.validate import validate_day, validate_name, validate_positive_id


@dataclass(frozen=True)
class OfficialReceipt:
    lake_id: str
    day: str
    revision: int


@dataclass(frozen=True)
class OfficialRow:
    key: ObservationKey
    partition: str
    source: str
    line: int
    mm: int | None
    position: int


@dataclass(frozen=True)
class OfficialRevision:
    receipt: OfficialReceipt
    parent: int | None
    operation_kind: str
    operation_id: str
    affected: tuple[str, ...]


def validate_official_receipt(receipt):
    if type(receipt) is not OfficialReceipt:
        raise ValueError('expected an official receipt')
    validate_name(receipt.lake_id, 'lake id')
    validate_day(receipt.day)
    validate_positive_id(receipt.revision, 'formal revision')
    return receipt


def initialize_ledger(db, lake_id, day):
    validate_name(lake_id, 'lake id')
    validate_day(day)
    with atomic(db):
        db.execute('''CREATE TABLE IF NOT EXISTS ledger_book (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            lake_id TEXT NOT NULL,
            day TEXT NOT NULL
        )''')
        db.execute('INSERT OR IGNORE INTO ledger_book VALUES (1, ?, ?)', (lake_id, day))
        if book_identity(db) != (lake_id, day):
            raise ValueError('source database is bound to another observation book')
        db.execute('''CREATE TABLE IF NOT EXISTS official_revisions (
            revision INTEGER PRIMARY KEY,
            parent INTEGER REFERENCES official_revisions(revision),
            operation_kind TEXT NOT NULL CHECK (operation_kind IN ('post', 'correction')),
            operation_id TEXT NOT NULL,
            affected TEXT NOT NULL,
            UNIQUE (operation_kind, operation_id)
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS official_rows (
            revision INTEGER NOT NULL REFERENCES official_revisions(revision),
            position INTEGER NOT NULL,
            batch_id TEXT NOT NULL,
            observation TEXT NOT NULL,
            partition TEXT NOT NULL,
            source TEXT NOT NULL,
            source_line INTEGER NOT NULL,
            mm TEXT,
            PRIMARY KEY (revision, position),
            UNIQUE (revision, batch_id, observation)
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS posted_batches (
            batch_id TEXT PRIMARY KEY REFERENCES intake_batches(batch_id),
            revision INTEGER NOT NULL UNIQUE REFERENCES official_revisions(revision)
        )''')


def book_identity(db):
    record = db.execute('SELECT lake_id, day FROM ledger_book WHERE singleton = 1').fetchone()
    if record is None:
        raise RuntimeError('source ledger is not initialized')
    return record[0], record[1]


def next_receipt(db, parent):
    number = 1 if parent is None else parent.receipt.revision + 1
    return OfficialReceipt(*book_identity(db), number)


def _revision_from_record(db, record):
    if record is None:
        return None
    number, parent, kind, operation_id, affected = record
    receipt = OfficialReceipt(*book_identity(db), number)
    return OfficialRevision(receipt, parent, kind, operation_id, tuple(json.loads(affected)))


def current_revision(db):
    record = db.execute('''SELECT revision, parent, operation_kind, operation_id, affected
        FROM official_revisions ORDER BY revision DESC LIMIT 1''').fetchone()
    return _revision_from_record(db, record)


def read_revision(db, revision):
    try:
        validate_positive_id(revision, 'formal revision')
    except ValueError:
        return None
    record = db.execute('''SELECT revision, parent, operation_kind, operation_id, affected
        FROM official_revisions WHERE revision = ?''', (revision,)).fetchone()
    return _revision_from_record(db, record)


def find_posted_receipt(db, batch_id):
    record = db.execute('SELECT revision FROM posted_batches WHERE batch_id = ?',
                        (batch_id,)).fetchone()
    return None if record is None else OfficialReceipt(*book_identity(db), record[0])


def resolve_official_receipt(db, receipt):
    try:
        validate_official_receipt(receipt)
    except ValueError:
        return None
    if book_identity(db) != (receipt.lake_id, receipt.day):
        return None
    return read_revision(db, receipt.revision)


def read_revision_rows(db, revision):
    records = db.execute('''SELECT batch_id, observation, partition, source, source_line, mm, position
        FROM official_rows WHERE revision = ? ORDER BY position''', (revision,))
    return tuple(OfficialRow(ObservationKey(batch_id, observation), partition, source,
                             line, decode_mm(mm), position)
                 for batch_id, observation, partition, source, line, mm, position in records)


def read_partition_changes(db, through_revision):
    records = db.execute('''SELECT revision, affected FROM official_revisions
        WHERE revision <= ? ORDER BY revision''', (through_revision,))
    return tuple((number, tuple(json.loads(affected))) for number, affected in records)


def correction_exists(db, correction_id):
    return db.execute('''SELECT 1 FROM official_revisions
        WHERE operation_kind = 'correction' AND operation_id = ?''',
                      (correction_id,)).fetchone() is not None


def save_revision(db, revision, rows, *, posted_batch=None):
    number = revision.receipt.revision
    db.execute('''INSERT INTO official_revisions
        (revision, parent, operation_kind, operation_id, affected) VALUES (?, ?, ?, ?, ?)''',
               (number, revision.parent, revision.operation_kind, revision.operation_id,
                json.dumps(revision.affected, separators=(',', ':'))))
    db.executemany('''INSERT INTO official_rows
        (revision, position, batch_id, observation, partition, source, source_line, mm)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (
        (number, row.position, row.key.batch_id, row.key.observation, row.partition,
         row.source, row.line, encode_mm(row.mm))
        for row in rows
    ))
    if posted_batch is not None:
        db.execute('INSERT INTO posted_batches VALUES (?, ?)', (posted_batch, number))
