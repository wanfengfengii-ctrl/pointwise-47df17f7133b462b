from contextlib import closing
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from lake.database import atomic, open_database
from lake.intake.source_rows import ObservationKey
from lake.ledger.revisions import OfficialRow
from lake.receiving.slots import FixedSlots, SlotRecord


class Interrupted(Exception):
    pass


class FixedSlotsTest(unittest.TestCase):
    def test_after_write_survives_directory_rollback(self):
        def interrupt(action, phase, index):
            if (action, phase, index) == ('replace', 'after', 2):
                raise Interrupted

        row = OfficialRow(ObservationKey('batch', 'R'), 'east', 'gage-A', 4, 1 << 70, 1)
        record = SlotRecord(row, 'write-1')
        with TemporaryDirectory() as tmp:
            directory = Path(tmp)
            catalog_path = directory / 'catalog.sqlite'
            medium_path = directory / 'medium.sqlite'
            with closing(open_database(catalog_path)) as catalog, \
                    closing(FixedSlots(medium_path, 2, hook=interrupt)) as medium:
                catalog.execute('CREATE TABLE operations (name TEXT NOT NULL)')
                self.assertEqual(medium.main_count, 2)
                self.assertEqual(medium.spare, 2)
                self.assertEqual([medium.read(i) for i in range(3)], [None, None, None])
                self.assertRaises(ValueError, medium.replace, 3, record)
                with self.assertRaises(Interrupted), atomic(catalog):
                    catalog.execute('INSERT INTO operations VALUES (?)', ('write-1',))
                    medium.replace(medium.spare, record)
            with closing(open_database(catalog_path)) as catalog, \
                    closing(FixedSlots(medium_path, 2)) as medium:
                self.assertEqual(catalog.execute('SELECT * FROM operations').fetchall(), [])
                self.assertEqual([medium.read(i) for i in range(3)], [None, None, record])
            with self.assertRaises(ValueError):
                FixedSlots(medium_path, 3)

    def test_hooks_straddle_the_single_slot_commit(self):
        trip = None

        def interrupt(action, phase, index):
            if (action, phase, index) == trip:
                raise Interrupted

        row = OfficialRow(ObservationKey('batch', 'T'), 'east', 'gage-C', 12, None, 3)
        original = SlotRecord(row, 'original')
        updated = SlotRecord(replace(row, mm=0), 'updated')
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / 'medium.sqlite'
            with closing(FixedSlots(path, 1, hook=interrupt)) as medium:
                medium.replace(0, original)
                medium.replace(medium.spare, updated)
                trip = ('replace', 'before', 0)
                with self.assertRaises(Interrupted):
                    medium.replace(0, updated)
                self.assertEqual(medium.read(0), original)
                trip = ('replace', 'after', 0)
                with self.assertRaises(Interrupted):
                    medium.replace(0, updated)
                self.assertEqual(medium.read(0), updated)
                trip = ('clear_spare', 'before', medium.spare)
                with self.assertRaises(Interrupted):
                    medium.clear_spare()
                self.assertEqual(medium.read(medium.spare), updated)
                trip = ('clear_spare', 'after', medium.spare)
                with self.assertRaises(Interrupted):
                    medium.clear_spare()
            with closing(FixedSlots(path, 1)) as medium:
                self.assertEqual(medium.read(0), updated)
                self.assertIsNone(medium.read(medium.spare))
