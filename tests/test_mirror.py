from dataclasses import replace
import json
import sqlite3

from lake.dispatch.publish import publish
from lake.dispatch.wire import decode_package, encode_package
from lake.intake.source_rows import ObservationKey
from lake.ledger.corrections import correct
from lake.ledger.history import source_read
from lake.ledger.revisions import OfficialReceipt
from lake.query.read import mirror_read
from lake.query.scope import InvalidMirrorRequest
from lake.query.visibility import PendingMaterials
from lake.receiving.accept import receive
from lake.receiving.receipt import PackageConflict, PackageRejected
from lake.receiving.store import initialize_mirror
from tests.support import ProjectCase, enter, initialize_source, observation, row_values, sample, subtotals


class TestMirror(ProjectCase):
    def transfer_releases(self):
        daily = sample('daily.json')
        change = sample('transfer.json')
        first = enter(self.source, daily['batch_id'], *daily['rows'])
        second = correct(self.source, change['correction_id'], first.revision,
                         ObservationKey(daily['batch_id'], change['observation']),
                         partition=change['partition'], mm=change['mm'])
        return publish(self.source, first), publish(self.source, second)

    def test_offline_read_and_replay(self):
        daily = sample('daily.json')
        receipt = enter(self.source, daily['batch_id'], *daily['rows'])
        scope = {'north', 'east'}
        expected = source_read(self.source, receipt.revision, scope)
        release = publish(self.source, receipt)
        self.source.close()
        mirror = self.open_mirror()
        north_id = receive(mirror, release.part('north'))
        east_id = receive(mirror, release.part('east'))
        self.assertNotEqual(north_id.receiving_id, east_id.receiving_id)
        mirror.close()
        mirror = self.open_mirror()
        self.assertEqual(receive(mirror, release.part('north')), north_id)
        self.assertEqual(receive(mirror, release.part('east')), east_id)
        actual = mirror_read(mirror, receipt, scope)
        self.assertEqual(actual.rows, expected.rows)
        self.assertEqual(actual.totals, expected.totals)
        self.assertEqual(actual.totals.total_mm, 12)
        self.assertEqual(actual.fulfilled_receipt, receipt)
        self.assertEqual(self.inventory_count(mirror), 2)

    def test_release_replay(self):
        original, moved = self.transfer_releases()
        self.assertEqual(publish(self.source, original.receipt), original)
        self.assertEqual(publish(self.source, moved.receipt), moved)
        self.assertEqual([name for name, _ in moved.parts], ['east', 'north', 'west'])
        with self.assertRaises(KeyError):
            original.part('west')
        self.assertEqual(self.source.execute('SELECT COUNT(*) FROM release_catalog').fetchone()[0], 2)

    def test_invalid_scope(self):
        receipt = enter(self.source, 'east', observation('E', 'east', 3))
        mirror = self.open_mirror()
        receive(mirror, publish(self.source, receipt).part('east'))
        for scope in ('east', [], ['bad name'], [True], {'east': True}):
            with self.subTest(scope=scope), self.assertRaises(InvalidMirrorRequest):
                mirror_read(mirror, receipt, scope)
        with self.assertRaises(InvalidMirrorRequest):
            mirror_read(mirror, OfficialReceipt(receipt.lake_id, receipt.day, True), ['east'])
        future = OfficialReceipt(receipt.lake_id, receipt.day, 2)
        self.assertIsInstance(mirror_read(mirror, future, ['east']), PendingMaterials)
        self.assertEqual(mirror_read(mirror, receipt, ['east', 'east']).totals.total_mm, 3)

    def test_zero_and_missing_parity(self):
        huge = (1 << 80) + 5
        receipt = enter(self.source, 'mixed', observation('zero', 'east', 0),
                        observation('missing', 'east', None, line=3),
                        observation('large', 'north', huge, line=4))
        scope = ['north', 'east', 'west']
        expected = source_read(self.source, receipt.revision, scope)
        release = publish(self.source, receipt)
        self.source.close()
        mirror = self.open_mirror()
        receive(mirror, release.part('north'))
        receive(mirror, release.part('east'))
        mirror.close()
        actual = mirror_read(self.open_mirror(), receipt, scope)
        self.assertEqual(actual.rows, expected.rows)
        self.assertEqual(actual.totals, expected.totals)
        self.assertEqual([row.mm for row in actual.rows], [0, None, huge])
        self.assertEqual(actual.totals.total_mm, huge)
        self.assertEqual(subtotals(actual), {'east': (0, 1, 2), 'north': (huge, 0, 1), 'west': (0, 0, 0)})

    def test_reinitialize(self):
        original, moved = self.transfer_releases()
        mirror = self.open_mirror()
        early = receive(mirror, original.part('east'))
        west = receive(mirror, moved.part('west'))
        initialize_source(self.source)
        self.assertEqual(publish(self.source, original.receipt), original)
        self.assertEqual(publish(self.source, moved.receipt), moved)
        self.assertEqual(source_read(self.source, 1, ['east']).totals.total_mm, 7)
        self.assertEqual(source_read(self.source, 2, ['west']).totals.total_mm, 11)
        self.source.close()
        initialize_mirror(mirror)
        mirror.close()
        mirror = self.open_mirror()
        self.assertEqual(receive(mirror, original.part('east')), early)
        self.assertEqual(receive(mirror, moved.part('west')), west)
        self.assertEqual(self.inventory_count(mirror), 2)
        self.assertEqual(mirror_read(mirror, original.receipt, ['east']).totals.total_mm, 7)
        self.assertEqual(mirror_read(mirror, moved.receipt, ['west']).totals.total_mm, 11)

    def test_damaged_part(self):
        original, moved = self.transfer_releases()
        mirror = self.open_mirror()
        receive(mirror, original.part('north'))
        message = json.loads(moved.part('west'))
        message['body']['rows'][0]['source'] = 'transport/altered'
        self.assertEqual(self.inventory_count(mirror), 1)
        with self.assertRaisesRegex(PackageRejected, 'digest mismatch'):
            receive(mirror, json.dumps(message).encode('utf-8'))
        self.assertEqual(self.inventory_count(mirror), 1)
        self.assertIsInstance(mirror_read(mirror, moved.receipt, ['west']), PendingMaterials)
        self.assertEqual(mirror_read(mirror, original.receipt, ['north']).totals.total_mm, 5)
        receive(mirror, moved.part('west'))
        self.assertEqual(mirror_read(mirror, moved.receipt, ['west']).rows[0].source, 'station-E/aug14')

    def test_same_id_conflict(self):
        original, _ = self.transfer_releases()
        mirror = self.open_mirror()
        payload = original.part('east')
        accepted = receive(mirror, payload)
        part = decode_package(payload)
        conflict = encode_package(replace(part, rows=(replace(part.rows[0], mm=13),)))
        with self.assertRaises(PackageConflict):
            receive(mirror, conflict)
        self.assertEqual(receive(mirror, json.dumps(json.loads(payload), indent=2).encode()), accepted)
        self.assertEqual(self.inventory_count(mirror), 1)
        account = mirror_read(mirror, original.receipt, ['east'])
        self.assertEqual(row_values(account.rows), [('R', 'east', 7, 'station-E/aug14', 4)])

    def test_receive_rollback(self):
        original, moved = self.transfer_releases()
        mirror = self.open_mirror()
        receive(mirror, original.part('north'))
        mirror.execute('''CREATE TEMP TRIGGER reject_projection_write
            BEFORE INSERT ON mirror_projections WHEN NEW.partition = 'west'
            BEGIN SELECT RAISE(ABORT, 'projection write stopped'); END''')
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'projection write stopped'):
            receive(mirror, moved.part('west'))
        self.assertEqual(self.inventory_count(mirror), 1)
        self.assertEqual(mirror.execute('SELECT COUNT(*) FROM mirror_projections').fetchone()[0], 1)
        self.assertIsInstance(mirror_read(mirror, moved.receipt, ['west']), PendingMaterials)
        self.assertEqual(mirror_read(mirror, original.receipt, ['north']).totals.total_mm, 5)
        mirror.execute('DROP TRIGGER reject_projection_write')
        receipt = receive(mirror, moved.part('west'))
        self.assertEqual(receive(mirror, moved.part('west')), receipt)
        self.assertEqual(self.inventory_count(mirror), 2)
        self.assertEqual(mirror_read(mirror, moved.receipt, ['west']).totals.total_mm, 11)

    def test_transfer_partial_and_complete(self):
        original, moved = self.transfer_releases()
        self.source.close()
        mirror = self.open_mirror()
        for partition in ('east', 'north'):
            receive(mirror, original.part(partition))
        east_west = {'east', 'west'}
        old = mirror_read(mirror, original.receipt, east_west)
        self.assertEqual(old.totals.total_mm, 7)
        receive(mirror, moved.part('west'))
        partial = mirror_read(mirror, moved.receipt, east_west)
        total = getattr(getattr(partial, 'totals', None), 'total_mm', None)
        self.assertIsInstance(partial, PendingMaterials,
                              f'MIRROR_TRANSFER_PARTIAL expected=pending actual_total_mm={total}')
        self.assertEqual(partial.requested_receipt, moved.receipt)
        self.assertFalse(hasattr(partial, 'rows'))
        self.assertEqual(mirror_read(mirror, original.receipt, ['north']).totals.total_mm, 5)
        self.assertEqual(mirror_read(mirror, moved.receipt, ['north']).totals.total_mm, 5)
        self.assertEqual(mirror_read(mirror, moved.receipt, ['west']).totals.total_mm, 11)
        self.assertIsInstance(mirror_read(mirror, moved.receipt, ['east']), PendingMaterials)
        receive(mirror, moved.part('east'))
        mirror.close()
        mirror = self.open_mirror()
        result = mirror_read(mirror, moved.receipt, east_west)
        self.assert_transfer_account(result, moved.receipt)
        self.assertEqual(mirror_read(mirror, original.receipt, ['north']).totals.total_mm, 5)
        self.assertEqual(mirror_read(mirror, original.receipt, east_west), old)

    def test_east_empty_part(self):
        original, moved = self.transfer_releases()
        part = decode_package(moved.part('east'))
        self.assertEqual(part.rows, ())
        self.assertEqual(part.checkpoint, moved.receipt.revision)
        self.assertEqual(part.manifest.affected, ('east', 'west'))
        mirror = self.open_mirror()
        receive(mirror, original.part('east'))
        receive(mirror, moved.part('east'))
        account = mirror_read(mirror, moved.receipt, ['east'])
        self.assertEqual(account.rows, ())
        self.assertEqual(subtotals(account), {'east': (0, 0, 0)})

    def test_reverse_order(self):
        original, moved = self.transfer_releases()
        self.source.close()
        mirror = self.open_mirror()
        east_id = receive(mirror, moved.part('east'))
        self.assertIsInstance(mirror_read(mirror, moved.receipt, ['east', 'west']), PendingMaterials)
        west_id = receive(mirror, moved.part('west'))
        receive(mirror, original.part('east'))
        receive(mirror, original.part('north'))
        expected = mirror_read(mirror, moved.receipt, ['east', 'west'])
        self.assert_transfer_account(expected, moved.receipt)
        mirror.close()
        mirror = self.open_mirror()
        self.assertEqual(receive(mirror, moved.part('west')), west_id)
        self.assertEqual(receive(mirror, moved.part('east')), east_id)
        self.assertEqual(mirror_read(mirror, moved.receipt, ['west', 'east']), expected)
        self.assertEqual(self.inventory_count(mirror), 4)

    def test_insert_is_not_pending_transfer(self):
        original, moved = self.transfer_releases()
        ordinary_db = self.open_source('ordinary.sqlite', 'ordinary')
        base = enter(ordinary_db, 'before', observation('A', 'east', 7), observation('B', 'north', 5))
        added = enter(ordinary_db, 'morning', observation('R', 'west', 11, source='station-E/aug14', line=4))
        old_ordinary, ordinary = publish(ordinary_db, base), publish(ordinary_db, added)
        self.source.close()
        ordinary_db.close()

        transfer_west = decode_package(moved.part('west'))
        ordinary_west = decode_package(ordinary.part('west'))
        self.assertEqual(row_values(transfer_west.rows), row_values(ordinary_west.rows))
        self.assertEqual(transfer_west.manifest.affected, ('east', 'west'))
        self.assertEqual(ordinary_west.manifest.affected, ('west',))
        self.assertEqual(transfer_west.manifest.checkpoint('east'), 2)
        self.assertEqual(ordinary_west.manifest.checkpoint('east'), 1)
        mirror = self.open_mirror()
        for first_release, final_release in ((original, moved), (old_ordinary, ordinary)):
            receive(mirror, first_release.part('east'))
            receive(mirror, final_release.part('west'))
        self.assertIsInstance(mirror_read(mirror, moved.receipt, ['east', 'west']), PendingMaterials)
        account = mirror_read(mirror, added, ['east', 'west'])
        self.assertEqual([row.key.observation for row in account.rows], ['A', 'R'])
        self.assertEqual(subtotals(account), {'east': (7, 0, 1), 'west': (11, 0, 1)})
        self.assertEqual(account.totals.total_mm, 18)

    def test_outgoing_material_must_distinguish_destination(self):
        pieces = []
        for destination in ('west', 'north'):
            db = self.open_source(f'outgoing-{destination}.sqlite')
            first = enter(db, 'origin', observation('R', 'east', 7),
                          observation('W', 'west', 4, line=2))
            new = correct(db, 'move', 1, ObservationKey('origin', 'R'), partition=destination, mm=11)
            base, latest = publish(db, first), publish(db, new)
            db.close()
            payload = latest.part('east')
            pieces.append(decode_package(payload))
            mirror = self.open_mirror(f'incoming-{destination}.sqlite')
            for old_partition in ('east', 'west'):
                receive(mirror, base.part(old_partition))
            receive(mirror, payload)
            result = mirror_read(mirror, new, {'east', 'west'})
            if destination == 'west':
                self.assertIsInstance(result, PendingMaterials)
            else:
                self.assertEqual(subtotals(result), {'east': (0, 0, 0), 'west': (4, 0, 1)})
                self.assertEqual(result.totals.total_mm, 4)
        self.assertEqual(pieces[0].package_id, pieces[1].package_id)
        self.assertEqual(pieces[0].manifest.receipt, pieces[1].manifest.receipt)
        self.assertEqual(pieces[0].rows, pieces[1].rows)
        self.assertNotEqual(pieces[0].manifest.coverage, pieces[1].manifest.coverage)

    def test_later_release_can_supply_a_complete_account(self):
        original, moved = self.transfer_releases()
        newer = correct(self.source, 'north-adjust', moved.receipt.revision,
                        ObservationKey('morning', 'B'), mm=6)
        later = publish(self.source, newer)
        self.source.close()
        mirror = self.open_mirror()
        receive(mirror, original.part('east'))
        receive(mirror, later.part('west'))
        self.assertIsInstance(mirror_read(mirror, moved.receipt, ['east', 'west']), PendingMaterials)
        receive(mirror, later.part('east'))
        result = mirror_read(mirror, moved.receipt, ['east', 'west'])
        self.assert_transfer_account(result, moved.receipt, fulfilled=newer)

    def assert_transfer_account(self, result, receipt, *, fulfilled=None):
        self.assertEqual(result.status, 'ready')
        self.assertEqual(result.requested_receipt, receipt)
        self.assertEqual(result.fulfilled_receipt, receipt if fulfilled is None else fulfilled)
        self.assertEqual(row_values(result.rows), [('R', 'west', 11, 'station-E/aug14', 4)])
        self.assertEqual(result.rows[0].position, 1)
        self.assertEqual(result.totals.total_mm, 11)
        self.assertEqual(subtotals(result), {'east': (0, 0, 0), 'west': (11, 0, 1)})

    @staticmethod
    def inventory_count(mirror):
        return mirror.execute('SELECT COUNT(*) FROM mirror_packages').fetchone()[0]
