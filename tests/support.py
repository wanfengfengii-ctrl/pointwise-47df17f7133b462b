import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from lake.database import open_database
from lake.dispatch.catalog import initialize_catalog
from lake.intake.batch import submit
from lake.intake.store import initialize_intake
from lake.ledger.commit import post
from lake.ledger.revisions import initialize_ledger
from lake.receiving.store import open_mirror


DAY = '2025-08-14'
LAKE_ID = 'survey'


def sample(name):
    path = Path(__file__).resolve().parents[1] / 'samples' / name
    return json.loads(path.read_text(encoding='utf-8'))


def observation(name, partition, mm, *, source='field-sheet', line=1):
    return {'observation': name, 'partition': partition, 'mm': mm,
            'source': source, 'line': line, 'unit': 'mm'}


def initialize_source(db, lake_id=LAKE_ID):
    initialize_intake(db)
    initialize_ledger(db, lake_id, DAY)
    initialize_catalog(db)


def enter(db, batch_id, *rows):
    submit(db, batch_id, rows)
    return post(db, batch_id)


def row_values(rows):
    return [(row.key.observation, row.partition, row.mm, row.source, row.line) for row in rows]


def subtotals(account):
    return {item.partition: (item.total_mm, item.missing_count, item.row_count)
            for item in account.totals.by_partition}


class ProjectCase(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.directory = Path(temp.name)
        self.source = self.open_source('source.sqlite')

    def open_source(self, filename, lake_id=LAKE_ID):
        db = open_database(self.directory / filename)
        self.addCleanup(db.close)
        initialize_source(db, lake_id)
        return db

    def open_mirror(self, filename='mirror.sqlite'):
        db = open_mirror(self.directory / filename)
        self.addCleanup(db.close)
        return db
