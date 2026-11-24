import sqlite3

from lake.intake.batch import submit
from lake.intake.receipt import BatchConflict, BatchRejected
from lake.intake.store import find_batch, read_accepted_rows
from lake.ledger.commit import ReceiptNotFound, post
from lake.ledger.history import RevisionNotFound, source_read
from lake.ledger.revisions import current_revision, find_posted_receipt, read_revision
from tests.support import ProjectCase, enter, initialize_source, observation, row_values, sample, subtotals


class TestDaily(ProjectCase):
    def test_post_and_audit(self):
        daily = sample('daily.json')
        intake = submit(self.source, daily['batch_id'], daily['rows'])
        self.assertEqual((intake.batch_id, intake.row_count), ('morning', 2))
        receipt = post(self.source, daily['batch_id'])
        self.assertEqual((receipt.lake_id, receipt.day, receipt.revision), ('survey', '2025-08-14', 1))
        account = source_read(self.source, receipt.revision, {'north', 'east'})
        self.assertEqual(row_values(account.rows), [
            ('R', 'east', 7, 'station-E/aug14', 4), ('B', 'north', 5, 'station-N/aug14', 9),
        ])
        self.assertEqual([row.position for row in account.rows], [1, 2])
        self.assertEqual(account.totals.total_mm, 12)
        self.assertEqual(subtotals(account), {'east': (7, 0, 1), 'north': (5, 0, 1)})
        with self.assertRaises(RevisionNotFound):
            source_read(self.source, 2, ['east'])

    def test_missing_and_repeat(self):
        data = [observation('real', 'east', 8), observation('missing', 'east', None, line=2)]
        original = submit(self.source, 'batch-a', data)
        receipt = post(self.source, 'batch-a')
        self.assertEqual(submit(self.source, 'batch-a', [dict(item) for item in data]), original)
        self.assertEqual(post(self.source, 'batch-a'), receipt)
        account = source_read(self.source, receipt.revision, ['east'])
        self.assertEqual([row.mm for row in account.rows], [8, None])
        self.assertEqual(account.totals.total_mm, 8)
        self.assertEqual(account.totals.missing_count, 1)
        self.assertEqual(account.rows[1].line, 2)
        self.assertIsNone(read_revision(self.source, 2))

    def test_zero_and_missing(self):
        receipt = enter(self.source, 'dry', observation('zero', 'east', 0),
                        observation('absent', 'east', None, line=7))
        account = source_read(self.source, receipt.revision, {'east', 'north'})
        self.assertEqual([(row.mm, row.line) for row in account.rows], [(0, 1), (None, 7)])
        self.assertEqual((account.totals.total_mm, account.totals.missing_count,
                          account.totals.row_count), (0, 1, 2))
        self.assertEqual(subtotals(account), {'east': (0, 1, 2), 'north': (0, 0, 0)})

    def test_repost_after_new_batch(self):
        one = enter(self.source, 'one', observation('A', 'east', 7))
        two = enter(self.source, 'two', observation('B', 'north', 5))
        self.assertEqual(post(self.source, 'one'), one)
        self.assertEqual(post(self.source, 'two'), two)
        self.assertEqual(current_revision(self.source).receipt, two)
        self.assertIsNone(read_revision(self.source, 3))
        self.assertEqual(source_read(self.source, one.revision, ['east', 'north']).totals.total_mm, 7)
        current = source_read(self.source, two.revision, ['north', 'east'])
        self.assertEqual([row.key.batch_id for row in current.rows], ['one', 'two'])
        self.assertEqual(current.totals.total_mm, 12)

    def test_reinitialize(self):
        first = enter(self.source, 'seed', observation('A', 'east', 3))
        pending = submit(self.source, 'user-batch', [observation('M', 'north', None)])
        initialize_source(self.source)
        self.source.close()
        db = self.open_source('source.sqlite')
        self.assertEqual(post(db, 'seed'), first)
        self.assertEqual(find_batch(db, 'user-batch'), pending)
        second = post(db, 'user-batch')
        account = source_read(db, second.revision, ['east', 'north'])
        self.assertEqual(subtotals(account), {'east': (3, 0, 1), 'north': (0, 1, 1)})

    def test_bad_input(self):
        receipt = enter(self.source, 'good', observation('A', 'east', 7))
        invalid = [dict(observation('X', 'east', 7), unit='cm'),
                   observation('X', 'east', True), observation('X', 'east', 7.0),
                   observation('X', 'east', 7, source=''), observation('X', 'east', 7, line=0)]
        without_mm = observation('X', 'east', None)
        del without_mm['mm']
        invalid.append(without_mm)
        for row in invalid:
            with self.subTest(row=row):
                with self.assertRaises(BatchRejected):
                    submit(self.source, 'bad', [observation('OK', 'east', 3), row])
                self.assertIsNone(find_batch(self.source, 'bad'))
        with self.assertRaises(BatchConflict):
            submit(self.source, 'good', [observation('A', 'east', 8)])
        with self.assertRaises(ReceiptNotFound):
            post(self.source, 'bad')
        self.assertEqual(current_revision(self.source).receipt, receipt)
        self.assertEqual(source_read(self.source, receipt.revision, ['east']).totals.total_mm, 7)
        self.assertEqual(self.source.execute('SELECT COUNT(*) FROM intake_batches').fetchone()[0], 1)
        self.assertEqual(self.source.execute('SELECT COUNT(*) FROM official_revisions').fetchone()[0], 1)

    def test_same_id_provenance(self):
        rows = [observation('A', 'east', 7, source='E/1'),
                observation('B', 'north', 5, source='N/1')]
        receipt = enter(self.source, 'original', *rows)
        with self.assertRaises(BatchConflict):
            submit(self.source, 'original', [dict(rows[0], source='E/2'), rows[1]])
        with self.assertRaises(BatchConflict):
            submit(self.source, 'original', list(reversed(rows)))
        self.assertEqual([row.source for row in read_accepted_rows(self.source, 'original')], ['E/1', 'N/1'])
        self.assertEqual(post(self.source, 'original'), receipt)
        account = source_read(self.source, receipt.revision, ['north', 'east'])
        self.assertEqual([row.source for row in account.rows], ['E/1', 'N/1'])
        self.assertEqual(account.totals.total_mm, 12)

    def test_post_rollback(self):
        first = enter(self.source, 'first', observation('A', 'east', 7))
        submit(self.source, 'retry', [observation('B', 'east', 5)])
        self.source.execute('''CREATE TEMP TRIGGER reject_post_binding
            BEFORE INSERT ON posted_batches WHEN NEW.batch_id = 'retry'
            BEGIN SELECT RAISE(ABORT, 'binding write stopped'); END''')
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'binding write stopped'):
            post(self.source, 'retry')
        self.assertIsNone(find_posted_receipt(self.source, 'retry'))
        self.assertIsNone(read_revision(self.source, 2))
        self.assertEqual(self.source.execute('SELECT COUNT(*) FROM official_rows').fetchone()[0], 1)
        self.assertEqual(source_read(self.source, first.revision, ['east']).totals.total_mm, 7)
        self.source.execute('DROP TRIGGER reject_post_binding')
        receipt = post(self.source, 'retry')
        self.assertEqual(receipt.revision, 2)
        self.assertEqual(source_read(self.source, receipt.revision, ['east']).totals.total_mm, 12)
