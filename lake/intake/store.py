from lake.database import atomic
from lake.intake.receipt import IntakeReceipt
from lake.intake.source_rows import ObservationKey, SourceRow, decode_mm, encode_mm


def initialize_intake(db):
    with atomic(db):
        db.execute('''CREATE TABLE IF NOT EXISTS intake_batches (
            batch_id TEXT PRIMARY KEY,
            row_count INTEGER NOT NULL CHECK (row_count > 0)
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS intake_rows (
            batch_id TEXT NOT NULL REFERENCES intake_batches(batch_id),
            position INTEGER NOT NULL,
            observation TEXT NOT NULL,
            partition TEXT NOT NULL,
            source TEXT NOT NULL,
            source_line INTEGER NOT NULL,
            mm TEXT,
            PRIMARY KEY (batch_id, position),
            UNIQUE (batch_id, observation)
        )''')


def find_batch(db, batch_id):
    record = db.execute('SELECT row_count FROM intake_batches WHERE batch_id = ?',
                        (batch_id,)).fetchone()
    return None if record is None else IntakeReceipt(batch_id, record[0])


def save_batch(db, receipt, rows):
    db.execute('INSERT INTO intake_batches VALUES (?, ?)',
               (receipt.batch_id, receipt.row_count))
    db.executemany('''INSERT INTO intake_rows
        (batch_id, position, observation, partition, source, source_line, mm)
        VALUES (?, ?, ?, ?, ?, ?, ?)''', (
        (row.key.batch_id, row.position, row.key.observation, row.partition,
         row.source, row.line, encode_mm(row.mm))
        for row in rows
    ))


def read_accepted_rows(db, batch_id):
    records = db.execute('''SELECT observation, partition, source, source_line, mm, position
        FROM intake_rows WHERE batch_id = ? ORDER BY position''', (batch_id,))
    return tuple(SourceRow(ObservationKey(batch_id, observation), partition, source,
                           line, decode_mm(mm), position)
                 for observation, partition, source, line, mm, position in records)
