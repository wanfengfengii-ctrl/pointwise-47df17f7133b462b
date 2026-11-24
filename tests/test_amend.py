import sqlite3

from lake.dispatch.publish import PublicationRejected, publish
from lake.dispatch.wire import decode_package
from lake.intake.source_rows import ObservationKey
from lake.ledger.corrections import CorrectionRejected, correct
from lake.ledger.history import source_read
from lake.ledger.revisions import OfficialReceipt
from tests.support import ProjectCase, enter, observation, row_values


class TestAmend(ProjectCase):
    def setUp(self):
        super().setUp()
        self.target = ObservationKey('morning', 'R')
        self.first = enter(self.source, 'morning', observation('R', 'east', 7, source='E/page', line=4))

    def test_frozen_source_reads(self):
        new = correct(self.source, 'adjust', self.first.revision, self.target, mm=9)
        for receipt, value in ((new, 9), (self.first, 7), (new, 9)):
            account = source_read(self.source, receipt.revision, ['east'])
            self.assertEqual(row_values(account.rows), [('R', 'east', value, 'E/page', 4)])
            self.assertEqual(account.rows[0].key, self.target)
            self.assertEqual(account.rows[0].position, 1)
            self.assertEqual(account.totals.total_mm, value)
            self.assertEqual(account.receipt, receipt)

    def test_historical_release(self):
        second = correct(self.source, 'new', self.first.revision, self.target, mm=9)
        current = publish(self.source, second)
        historic = publish(self.source, self.first)
        old = decode_package(historic.part('east'))
        new = decode_package(current.part('east'))
        self.assertNotEqual(current.release_id, historic.release_id)
        self.assertEqual(old.manifest.receipt, self.first)
        self.assertEqual((old.checkpoint, new.checkpoint), (1, 2))
        self.assertEqual(row_values(old.rows), [('R', 'east', 7, 'E/page', 4)])
        self.assertEqual(row_values(new.rows), [('R', 'east', 9, 'E/page', 4)])

    def test_bad_parent(self):
        prior = publish(self.source, self.first)
        with self.assertRaises(CorrectionRejected):
            correct(self.source, 'bad-parent', 99, self.target, mm=13)
        with self.assertRaises(PublicationRejected):
            publish(self.source, OfficialReceipt(self.first.lake_id, self.first.day, 2))
        with self.assertRaises(PublicationRejected):
            publish(self.source, OfficialReceipt('other-lake', self.first.day, 1))
        self.assert_counts(revisions=1, releases=1)
        self.assertEqual(publish(self.source, self.first), prior)
        self.assertEqual(source_read(self.source, 1, ['east']).totals.total_mm, 7)

    def test_stale_parent(self):
        new = correct(self.source, 'new', 1, self.target, mm=9)
        release = publish(self.source, new)
        with self.assertRaises(CorrectionRejected):
            correct(self.source, 'stale', 1, self.target, mm=13)
        self.assert_counts(revisions=2, releases=1)
        self.assertEqual(publish(self.source, new), release)
        self.assertEqual(source_read(self.source, 1, ['east']).totals.total_mm, 7)
        self.assertEqual(source_read(self.source, 2, ['east']).totals.total_mm, 9)

    def test_bad_target(self):
        prior = publish(self.source, self.first)
        with self.assertRaises(CorrectionRejected):
            correct(self.source, 'bad-target', 1, ObservationKey('morning', 'X'), partition='west')
        self.assert_counts(revisions=1, releases=1)
        self.assertEqual(publish(self.source, self.first), prior)
        self.assertEqual(source_read(self.source, 1, ['east', 'west']).totals.total_mm, 7)

    def test_release_rollback(self):
        moved = correct(self.source, 'move', 1, self.target, partition='west', mm=11)
        self.source.execute('''CREATE TEMP TRIGGER reject_west_part
            BEFORE INSERT ON release_parts WHEN NEW.partition = 'west'
            BEGIN SELECT RAISE(ABORT, 'west write stopped'); END''')
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'west write stopped'):
            publish(self.source, moved)
        self.assert_counts(revisions=2, releases=0)
        self.assertEqual(self.source.execute('SELECT COUNT(*) FROM release_parts').fetchone()[0], 0)
        self.source.execute('DROP TRIGGER reject_west_part')
        release = publish(self.source, moved)
        self.assertEqual(tuple(name for name, _ in release.parts), ('east', 'west'))
        self.assertEqual(decode_package(release.part('east')).rows, ())

    def assert_counts(self, *, revisions, releases):
        self.assertEqual(self.source.execute('SELECT COUNT(*) FROM official_revisions').fetchone()[0],
                         revisions)
        self.assertEqual(self.source.execute('SELECT COUNT(*) FROM release_catalog').fetchone()[0],
                         releases)
