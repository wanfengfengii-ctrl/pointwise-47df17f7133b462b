"""Independent fixed-slot medium for the cold mirror."""
from dataclasses import dataclass

from lake.database import atomic, open_database
from lake.intake.source_rows import ObservationKey, decode_mm, encode_mm
from lake.intake.validate import (
    validate_mm, validate_name, validate_positive_id, validate_source,
)
from lake.ledger.revisions import OfficialRow


@dataclass(frozen=True)
class SlotRecord:
    row: OfficialRow
    marker: str | None = None


class FixedSlots:
    """Expose only per-slot persistence, on a connection owned by the medium."""

    def __init__(self, path, main_count, *, hook=None):
        self._main_count = validate_positive_id(main_count, 'main slot count')
        self._hook = hook
        self._db = open_database(path)
        try:
            self._db.execute('PRAGMA synchronous = FULL')
            with atomic(self._db) as db:
                db.execute('''CREATE TABLE IF NOT EXISTS medium_layout (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    main_count INTEGER NOT NULL CHECK (main_count > 0)
                )''')
                db.execute('INSERT OR IGNORE INTO medium_layout VALUES (1, ?)', (main_count,))
                stored, = db.execute('SELECT main_count FROM medium_layout').fetchone()
                if stored != main_count:
                    raise ValueError('medium has a different slot count')
                db.execute('''CREATE TABLE IF NOT EXISTS medium_slots (
                    address INTEGER PRIMARY KEY,
                    batch_id TEXT NOT NULL, observation TEXT NOT NULL,
                    partition TEXT NOT NULL, source TEXT NOT NULL,
                    source_line INTEGER NOT NULL, position INTEGER NOT NULL,
                    mm TEXT, marker TEXT
                )''')
        except BaseException:
            self._db.close()
            raise

    @property
    def main_count(self):
        return self._main_count

    @property
    def spare(self):
        return self._main_count

    def close(self):
        self._db.close()

    def read(self, index):
        self._check_address(index)
        result = self._db.execute('''SELECT batch_id, observation, partition, source,
            source_line, position, mm, marker FROM medium_slots WHERE address = ?''',
                                  (index,)).fetchone()
        if result is None:
            return None
        batch, observation, partition, source, line, position, mm, marker = result
        row = OfficialRow(ObservationKey(batch, observation), partition, source,
                          line, decode_mm(mm), position)
        return SlotRecord(row, marker)

    def replace(self, index, record):
        self._check_address(index)
        if type(record) is not SlotRecord or type(record.row) is not OfficialRow:
            raise ValueError('expected a slot record with an official row')
        row = record.row
        if type(row.key) is not ObservationKey:
            raise ValueError('expected an observation key')
        validate_name(row.key.batch_id, 'row batch')
        validate_name(row.key.observation, 'row observation')
        validate_name(row.partition, 'row partition')
        validate_source(row.source)
        validate_positive_id(row.line, 'source line')
        validate_positive_id(row.position, 'formal row position')
        validate_mm(row.mm)
        marker = record.marker
        if marker is not None and (type(marker) is not str or not marker.isprintable()
                                   or not 1 <= len(marker) <= 256):
            raise ValueError('invalid slot marker')
        self._notify('replace', 'before', index)
        with atomic(self._db) as db:
            db.execute('''INSERT OR REPLACE INTO medium_slots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                       (index, row.key.batch_id, row.key.observation, row.partition,
                        row.source, row.line, row.position, encode_mm(row.mm), marker))
        self._notify('replace', 'after', index)

    def clear_spare(self):
        self._notify('clear_spare', 'before', self.spare)
        with atomic(self._db) as db:
            db.execute('DELETE FROM medium_slots WHERE address = ?', (self.spare,))
        self._notify('clear_spare', 'after', self.spare)

    def _check_address(self, index):
        if type(index) is not int or not 0 <= index <= self.spare:
            raise ValueError('invalid slot address')

    def _notify(self, action, phase, index):
        if self._hook is not None:
            self._hook(action, phase, index)
