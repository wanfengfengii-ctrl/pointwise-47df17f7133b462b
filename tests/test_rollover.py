"""Tests for the cold-mirror rollover (page swap) workflow."""
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

from lake.database import open_database
from lake.dispatch.publish import publish, publish_rollover
from lake.dispatch.rollover_wire import decode_rollover, encode_rollover
from lake.intake.source_rows import ObservationKey
from lake.ledger.corrections import correct
from lake.ledger.history import source_read
from lake.ledger.revisions import OfficialReceipt
from lake.query.read import read_rollover
from lake.receiving.accept import (
    initialize_rollover, receive_rollover, retire_rollover,
)
from lake.receiving.rollover import RolloverRejected
from lake.receiving.slots import FixedSlots
from lake.receiving.store import open_mirror
from tests.support import (
    DAY, LAKE_ID, ProjectCase, enter, observation, row_values, subtotals,
)


class Interrupted(Exception):
    pass


class RolloverCase(ProjectCase):
    def setUp(self):
        super().setUp()
        self.rows = [
            observation('R', 'east', 7, source='gage-A', line=4),
            observation('S', 'east', 7, source='gage-B', line=9),
            observation('T', 'east', None, source='gage-C', line=12),
        ]
        self.old = enter(self.source, 'roll', *self.rows)
        self.old_release = publish(self.source, self.old)
        self.new = correct(self.source, 'remeasure-R', self.old.revision,
                           ObservationKey('roll', 'R'), mm=11)
        self.payload = publish_rollover(self.source, self.old, self.new, 'east')
        self.mirror_path = self.directory / 'cold.sqlite'
        self.medium_path = self.directory / 'medium.sqlite'

    def build(self, *, hook=None):
        mirror = open_mirror(self.mirror_path)
        self.addCleanup(mirror.close)
        medium = FixedSlots(self.medium_path, 3)
        self.addCleanup(medium.close)
        receipt = initialize_rollover(mirror, medium, self.old_release.part('east'))
        medium._hook = hook
        return mirror, medium, receipt

    def reopen(self, *, hook=None):
        mirror = open_mirror(self.mirror_path)
        self.addCleanup(mirror.close)
        medium = FixedSlots(self.medium_path, 3, hook=hook)
        self.addCleanup(medium.close)
        return mirror, medium

    def inventory(self, mirror):
        return mirror.execute('SELECT COUNT(*) FROM rollover_rows').fetchone()[0]


class TestBuildAndRead(RolloverCase):
    def test_build_from_old_partition(self):
        mirror, medium, receipt = self.build()
        self.assertEqual(receipt, self.old)
        self.assertEqual(medium.main_count, 3)
        self.assertEqual([medium.read(i).row.mm for i in range(3)], [7, 7, None])
        self.assertEqual([medium.read(i).row.key.observation for i in range(3)], ['R', 'S', 'T'])
        self.assertIsNone(medium.read(medium.spare))
        account = read_rollover(mirror, medium, self.old)
        self.assertEqual(account.status, 'ready')
        self.assertEqual(account.fulfilled_receipt, self.old)
        self.assertEqual(row_values(account.rows), [
            ('R', 'east', 7, 'gage-A', 4), ('S', 'east', 7, 'gage-B', 9),
            ('T', 'east', None, 'gage-C', 12),
        ])
        self.assertEqual([row.position for row in account.rows], [1, 2, 3])
        self.assertEqual(account.totals.total_mm, 14)
        self.assertEqual(account.totals.missing_count, 1)

    def test_new_pending_before_delivery(self):
        mirror, medium, _ = self.build()
        pending = read_rollover(mirror, medium, self.new)
        self.assertEqual(pending.status, 'pending')
        self.assertEqual(pending.requested_receipt, self.new)
        self.assertFalse(hasattr(pending, 'rows'))

    def test_unknown_receipt_pending(self):
        mirror, medium, _ = self.build()
        future = OfficialReceipt(LAKE_ID, DAY, 9)
        self.assertEqual(read_rollover(mirror, medium, future).status, 'pending')

    def test_build_requires_matching_medium(self):
        mirror = open_mirror(self.mirror_path)
        self.addCleanup(mirror.close)
        medium = FixedSlots(self.medium_path, 2)
        self.addCleanup(medium.close)
        with self.assertRaises(ValueError):
            initialize_rollover(mirror, medium, self.old_release.part('east'))
        self.assertEqual(self.inventory(mirror), 0)

    def test_rebuild_is_stable(self):
        mirror, medium, receipt = self.build()
        mirror.close()
        medium.close()
        mirror, medium = self.reopen()
        self.assertEqual(initialize_rollover(mirror, medium, self.old_release.part('east')),
                         receipt)
        medium.replace(0, type(medium.read(0))(replace(medium.read(0).row, mm=99)))
        with self.assertRaises(ValueError):
            initialize_rollover(mirror, medium, self.old_release.part('east'))


class TestAccept(RolloverCase):
    def test_accept_then_both_versions(self):
        mirror, medium, _ = self.build()
        first = receive_rollover(mirror, medium, self.payload)
        self.assertEqual(first.package_id, decode_rollover(self.payload).package_id)
        old = read_rollover(mirror, medium, self.old)
        new = read_rollover(mirror, medium, self.new)
        self.assertEqual(old.totals.total_mm, 14)
        self.assertEqual(new.totals.total_mm, 18)
        self.assertEqual(new.fulfilled_receipt, self.new)
        self.assertEqual([row.mm for row in new.rows], [11, 7, None])
        self.assertEqual([row.position for row in new.rows], [1, 2, 3])
        self.assertEqual(new.rows[0].source, 'gage-A')
        self.assertEqual(new.rows[0].line, 4)
        self.assertEqual(new.totals.missing_count, 1)
        self.assertEqual(subtotals(new), {'east': (18, 1, 3)})

    def test_replay_first_receipt(self):
        mirror, medium, _ = self.build()
        receipt = receive_rollover(mirror, medium, self.payload)
        self.assertEqual(receive_rollover(mirror, medium, self.payload), receipt)
        pretty = json.dumps(json.loads(self.payload), indent=2).encode('utf-8')
        self.assertEqual(receive_rollover(mirror, medium, pretty), receipt)

    def test_survives_reopen(self):
        mirror, medium, _ = self.build()
        first = receive_rollover(mirror, medium, self.payload)
        mirror.close()
        medium.close()
        mirror, medium = self.reopen()
        self.assertEqual(receive_rollover(mirror, medium, self.payload), first)
        self.assertEqual(read_rollover(mirror, medium, self.old).totals.total_mm, 14)
        self.assertEqual(read_rollover(mirror, medium, self.new).totals.total_mm, 18)

    def test_bad_bytes_leave_state(self):
        mirror, medium, _ = self.build()
        broken = json.loads(self.payload)
        broken['body']['row']['source'] = 'altered'
        with self.assertRaises(ValueError):
            receive_rollover(mirror, medium, json.dumps(broken).encode())
        with self.assertRaises(ValueError):
            receive_rollover(mirror, medium, b'not json')
        self.assertEqual(read_rollover(mirror, medium, self.new).status, 'pending')
        self.assertEqual(read_rollover(mirror, medium, self.old).totals.total_mm, 14)
        self.assertIsNone(medium.read(medium.spare))
        receive_rollover(mirror, medium, self.payload)
        self.assertEqual(read_rollover(mirror, medium, self.new).totals.total_mm, 18)

    def test_conflicting_package_rejected(self):
        mirror, medium, _ = self.build()
        received = receive_rollover(mirror, medium, self.payload)
        package = decode_rollover(self.payload)
        tampered = encode_rollover(replace(package, row=replace(package.row, mm=13)))
        with self.assertRaises(ValueError):
            receive_rollover(mirror, medium, tampered)
        self.assertEqual(receive_rollover(mirror, medium, self.payload), received)
        self.assertEqual(read_rollover(mirror, medium, self.new).totals.total_mm, 18)

    def test_tampered_credentials_rejected(self):
        mirror, medium, _ = self.build()
        package = decode_rollover(self.payload)
        for label, broken in (
                ('neighbour', replace(package, target=replace(
                    package.target, before_digest='0' * 64))),
                ('target-digest', replace(package, target=replace(
                    package.target, old_digest='f' * 64))),
                ('old-order', replace(package, old_order=tuple(
                    reversed(package.old_order)))),
                ('old-digest', replace(package, old_digest='e' * 64)),
                ('new-digest', replace(package, new_digest='d' * 64)),
        ):
            with self.subTest(label=label), self.assertRaises(ValueError):
                receive_rollover(mirror, medium, encode_rollover(broken))
            self.assertEqual(read_rollover(mirror, medium, self.new).status, 'pending')
            self.assertIsNone(medium.read(medium.spare))
        receive_rollover(mirror, medium, self.payload)
        self.assertEqual(read_rollover(mirror, medium, self.new).totals.total_mm, 18)

    def test_wrong_medium_binding_rejected(self):
        mirror, medium, _ = self.build()
        with self.assertRaises(ValueError):
            receive_rollover(mirror, medium, b'{not json')
        with closing(open_mirror(self.directory / 'other.sqlite')) as other_mirror:
            with self.assertRaises(ValueError):
                receive_rollover(other_mirror, medium, self.payload)
        self.assertEqual(read_rollover(mirror, medium, self.new).status, 'pending')
        self.assertEqual(read_rollover(mirror, medium, self.old).totals.total_mm, 14)

    def test_second_package_rejected(self):
        mirror, medium, _ = self.build()
        receive_rollover(mirror, medium, self.payload)
        later = correct(self.source, 'remeasure-S', self.new.revision,
                        ObservationKey('roll', 'S'), mm=9)
        payload = publish_rollover(self.source, self.new, later, 'east')
        with self.assertRaises(ValueError):
            receive_rollover(mirror, medium, payload)
        self.assertEqual(read_rollover(mirror, medium, self.new).totals.total_mm, 18)


class TestPublication(RolloverCase):
    def test_repeated_publication_is_frozen(self):
        self.assertEqual(publish_rollover(self.source, self.old, self.new, 'east'), self.payload)
        self.assertEqual(publish_rollover(self.source, self.old, self.new, 'east'), self.payload)

    def test_rejects_non_rollover(self):
        same_value = correct(self.source, 'same', self.new.revision,
                             ObservationKey('roll', 'S'), mm=7)
        with self.assertRaises(ValueError):
            publish_rollover(self.source, self.new, same_value, 'east')
        with self.assertRaises(ValueError):
            publish_rollover(self.source, self.old, self.new, 'north')
        with self.assertRaises(ValueError):
            publish_rollover(self.source, self.old, self.old, 'east')

        with self.assertRaises(ValueError):
            publish_rollover(self.source, OfficialReceipt(LAKE_ID, DAY, 99), self.new, 'east')

    def test_rejects_move(self):
        moved = correct(self.source, 'move', self.new.revision,
                        ObservationKey('roll', 'R'), partition='west', mm=11)
        with self.assertRaises(ValueError):
            publish_rollover(self.source, self.new, moved, 'east')

    def test_single_changed_value(self):
        package = decode_rollover(self.payload)
        self.assertEqual(package.row.key, ObservationKey('roll', 'R'))
        self.assertEqual(package.row.mm, 11)
        self.assertEqual([entry.key.observation for entry in package.old_order], ['R', 'S', 'T'])
        self.assertEqual([entry.position for entry in package.old_order], [1, 2, 3])


class TestRetire(RolloverCase):
    def accepted(self):
        mirror, medium, _ = self.build()
        receive_rollover(mirror, medium, self.payload)
        return mirror, medium

    def test_retire_success(self):
        mirror, medium = self.accepted()
        self.assertEqual(retire_rollover(mirror, medium, 'close-old', self.old),
                         ('close-old', self.old, self.new))
        self.assertEqual(read_rollover(mirror, medium, self.old).status, 'retired')
        self.assertEqual(read_rollover(mirror, medium, self.new).totals.total_mm, 18)
        self.assertIsNone(medium.read(medium.spare))
        self.assertEqual(retire_rollover(mirror, medium, 'close-old', self.old),
                         ('close-old', self.old, self.new))
        self.assertEqual(read_rollover(mirror, medium, self.old).status, 'retired')

    def test_replay_after_retirement(self):
        mirror, medium = self.accepted()
        receipt = receive_rollover(mirror, medium, self.payload)
        retire_rollover(mirror, medium, 'close-old', self.old)
        self.assertEqual(receive_rollover(mirror, medium, self.payload), receipt)
        self.assertEqual(read_rollover(mirror, medium, self.old).status, 'retired')
        self.assertEqual(read_rollover(mirror, medium, self.new).totals.total_mm, 18)

    def test_retired_old_has_no_rows(self):
        mirror, medium = self.accepted()
        retire_rollover(mirror, medium, 'close-old', self.old)
        retired = read_rollover(mirror, medium, self.old)
        self.assertEqual(retired.status, 'retired')
        self.assertEqual(retired.requested_receipt, self.old)
        self.assertFalse(hasattr(retired, 'rows'))

    def test_retire_refuses_when_main_slots_do_not_serve_new(self):
        mirror, medium = self.accepted()
        # Corrupt the target main slot so the new version is no longer served.
        medium.replace(0, type(medium.read(0))(replace(medium.read(0).row, mm=7)))
        with self.assertRaises(ValueError):
            retire_rollover(mirror, medium, 'close-old', self.old)
        self.assertEqual(read_rollover(mirror, medium, self.old).status, 'ready')
        self.assertEqual(read_rollover(mirror, medium, self.old).totals.total_mm, 14)

    def test_retire_requires_acceptance(self):
        mirror, medium, _ = self.build()
        with self.assertRaises(ValueError):
            retire_rollover(mirror, medium, 'close-old', self.old)
        self.assertEqual(read_rollover(mirror, medium, self.old).status, 'ready')

    def test_retire_names_right_receipt(self):
        mirror, medium = self.accepted()
        with self.assertRaises(ValueError):
            retire_rollover(mirror, medium, 'close-old', self.new)
        self.assertEqual(read_rollover(mirror, medium, self.old).status, 'ready')

    def test_conflicting_operation(self):
        mirror, medium = self.accepted()
        retire_rollover(mirror, medium, 'close-old', self.old)
        with self.assertRaises(ValueError):
            retire_rollover(mirror, medium, 'other', self.old)


class TestInterruption(RolloverCase):
    def test_accept_interrupted_after_spare_write(self):
        trip = ('replace', 'after', 3)
        errors = []

        def hook(action, phase, index):
            if (action, phase, index) == trip:
                raise Interrupted

        mirror, medium, _ = self.build(hook=hook)
        with self.assertRaises(Interrupted):
            receive_rollover(mirror, medium, self.payload)
        # Old body is backed up, main target still old; no acceptance yet.
        self.assertEqual(read_rollover(mirror, medium, self.new).status, 'pending')
        self.assertEqual(read_rollover(mirror, medium, self.old).totals.total_mm, 14)
        receipt = receive_rollover(mirror, medium, self.payload)
        self.assertEqual(read_rollover(mirror, medium, self.new).totals.total_mm, 18)
        self.assertEqual(receive_rollover(mirror, medium, self.payload), receipt)

    def test_accept_interrupted_after_main_write(self):
        def hook(action, phase, index):
            if (action, phase, index) == ('replace', 'after', 0):
                raise Interrupted

        mirror, medium, _ = self.build(hook=hook)
        with self.assertRaises(Interrupted):
            receive_rollover(mirror, medium, self.payload)
        self.assertEqual(read_rollover(mirror, medium, self.new).status, 'pending')
        self.assertEqual(read_rollover(mirror, medium, self.old).totals.total_mm, 14)
        self.assertEqual(read_rollover(mirror, medium, self.old).rows[0].mm, 7)
        mirror.close()
        medium.close()
        mirror, medium = self.reopen()
        receive_rollover(mirror, medium, self.payload)
        self.assertEqual(read_rollover(mirror, medium, self.new).totals.total_mm, 18)
        self.assertEqual(read_rollover(mirror, medium, self.old).totals.total_mm, 14)

    def test_retire_interrupted_after_mark_before_spare_clear(self):
        trip = ('clear_spare', 'before', 3)

        def hook(action, phase, index):
            if (action, phase, index) == trip:
                raise Interrupted

        mirror, medium, _ = self.build(hook=hook)
        receive_rollover(mirror, medium, self.payload)
        with self.assertRaises(Interrupted):
            retire_rollover(mirror, medium, 'close-old', self.old)
        # The directory mark was committed before the spare slot was touched.
        self.assertEqual(read_rollover(mirror, medium, self.old).status, 'retired')
        self.assertIsNotNone(medium.read(medium.spare))
        mirror.close()
        medium.close()
        mirror, medium = self.reopen()
        retire_rollover(mirror, medium, 'close-old', self.old)
        self.assertIsNone(medium.read(medium.spare))
        self.assertEqual(read_rollover(mirror, medium, self.old).status, 'retired')

    def test_retire_interrupted_after_spare_clear(self):
        trip = ('clear_spare', 'after', 3)

        def hook(action, phase, index):
            if (action, phase, index) == trip:
                raise Interrupted

        mirror, medium, _ = self.build(hook=hook)
        receive_rollover(mirror, medium, self.payload)
        with self.assertRaises(Interrupted):
            retire_rollover(mirror, medium, 'close-old', self.old)
        # The first 'retired' report is permanent; resuming finishes the bookkeeping.
        self.assertEqual(read_rollover(mirror, medium, self.old).status, 'retired')
        mirror.close()
        medium.close()
        mirror, medium = self.reopen()
        self.assertEqual(retire_rollover(mirror, medium, 'close-old', self.old),
                         ('close-old', self.old, self.new))
        self.assertIsNone(medium.read(medium.spare))
        self.assertEqual(read_rollover(mirror, medium, self.new).totals.total_mm, 18)

    def test_same_operation_id_different_arguments(self):
        mirror, medium, _ = self.build()
        receive_rollover(mirror, medium, self.payload)
        retire_rollover(mirror, medium, 'close-old', self.old)
        with self.assertRaises(ValueError):
            retire_rollover(mirror, medium, 'close-old', self.new)

    def test_spare_carrying_new_body_blocks_success(self):
        mirror, medium, _ = self.build()
        receive_rollover(mirror, medium, self.payload)

        def interrupt(action, phase, index):
            if (action, phase, index) == ('clear_spare', 'before', 3):
                raise Interrupted

        medium._hook = interrupt
        with self.assertRaises(Interrupted):
            retire_rollover(mirror, medium, 'close-old', self.old)
        # Simulate a bad actor putting an unexpected body into the spare slot.
        new_body = medium.read(0).row
        medium.replace(medium.spare, type(medium.read(0))(replace(new_body, mm=11)))
        with self.assertRaises(ValueError):
            retire_rollover(mirror, medium, 'close-old', self.old)
        self.assertEqual(read_rollover(mirror, medium, self.old).status, 'retired')


class TestSecondSample(ProjectCase):
    def test_missing_to_zero(self):
        rows = [
            observation('N', 'north', 5, source='gage-N', line=2),
            observation('T', 'east', None, source='gage-C', line=12),
            observation('S', 'east', 7, source='gage-B', line=9),
        ]
        old = enter(self.source, 'zero', *rows)
        release = publish(self.source, old)
        new = correct(self.source, 'remeasure-T', old.revision,
                      ObservationKey('zero', 'T'), mm=0)
        payload = publish_rollover(self.source, old, new, 'east')
        mirror = open_mirror(self.directory / 'mirror.sqlite')
        self.addCleanup(mirror.close)
        medium = FixedSlots(self.directory / 'medium.sqlite', 2)
        self.addCleanup(medium.close)
        initialize_rollover(mirror, medium, release.part('east'))
        receive_rollover(mirror, medium, payload)
        old_account = read_rollover(mirror, medium, old)
        new_account = read_rollover(mirror, medium, new)
        self.assertEqual([row.key.observation for row in old_account.rows], ['T', 'S'])
        self.assertEqual([row.position for row in old_account.rows], [2, 3])
        self.assertEqual([row.mm for row in new_account.rows], [0, 7])
        self.assertEqual([row.position for row in new_account.rows], [2, 3])
        self.assertEqual((old_account.totals.total_mm, old_account.totals.missing_count), (7, 1))
        self.assertEqual((new_account.totals.total_mm, new_account.totals.missing_count), (7, 0))


class TestDirectoryBoundary(RolloverCase):
    def test_no_bodies_in_directory(self):
        mirror, medium, _ = self.build()
        receive_rollover(mirror, medium, self.payload)
        dump = '\n'.join(mirror.iterdump())
        for needle in ('gage-A', 'gage-B', 'gage-C', 'score', 'line'):
            self.assertNotIn(needle, dump)
        self.assertNotIn('rollover/', dump)

    def test_medium_write_cannot_be_rolled_back_by_directory(self):
        mirror, medium, _ = self.build()
        mirror.execute('''CREATE TEMP TRIGGER stop_accept BEFORE INSERT ON rollover_acceptance
            BEGIN SELECT RAISE(ABORT, 'accept stopped'); END''')
        with self.assertRaises(Exception):
            receive_rollover(mirror, medium, self.payload)
        self.assertEqual(read_rollover(mirror, medium, self.new).status, 'pending')
        mirror.execute('DROP TRIGGER stop_accept')
        receive_rollover(mirror, medium, self.payload)
        self.assertEqual(read_rollover(mirror, medium, self.new).totals.total_mm, 18)
