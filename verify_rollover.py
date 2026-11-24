"""End-to-end checks for the cold-mirror rollover, wired into verify.

This runs the two documented sample groups plus the interruption matrix over
one independent source database and one cold mirror each, then prints a
machine-readable summary.
"""
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from lake.database import open_database
from lake.dispatch.catalog import initialize_catalog
from lake.dispatch.publish import publish, publish_rollover
from lake.intake.batch import submit
from lake.intake.source_rows import ObservationKey
from lake.intake.store import initialize_intake
from lake.ledger.commit import post
from lake.ledger.corrections import correct
from lake.ledger.revisions import initialize_ledger
from lake.query.read import read_rollover
from lake.receiving.accept import (
    initialize_rollover, receive_rollover, retire_rollover,
)
from lake.receiving.slots import FixedSlots
from lake.receiving.store import open_mirror

SAMPLE = Path(__file__).resolve().parent / 'samples' / 'rollover.json'


class VerificationError(AssertionError):
    pass


def _expect(condition, message):
    if not condition:
        raise VerificationError(message)


def _load_case(index):
    cases = json.loads(SAMPLE.read_text(encoding='utf-8'))
    return cases[index]


def _prepare_source(source, case):
    initialize_intake(source)
    initialize_ledger(source, case['lake_id'], case['day'])
    initialize_catalog(source)
    submit(source, case['batch_id'], case['rows'])
    old = post(source, case['batch_id'])
    old_release = publish(source, old)
    change = case['change']
    new = correct(source, change['correction_id'], old.revision,
                  ObservationKey(case['batch_id'], change['observation']),
                  mm=change['mm'])
    payload = publish_rollover(source, old, new, case['partition'])
    return old, old_release, new, payload


def _view(account):
    return {
        'status': account.status,
        'requested': account.requested_receipt.revision,
        'fulfilled': account.fulfilled_receipt.revision,
        'observations': [row.key.observation for row in account.rows],
        'partitions': [row.partition for row in account.rows],
        'positions': [row.position for row in account.rows],
        'mm': [row.mm for row in account.rows],
        'sources': [row.source for row in account.rows],
        'lines': [row.line for row in account.rows],
        'total_mm': account.totals.total_mm,
        'missing_count': account.totals.missing_count,
        'row_count': account.totals.row_count,
    }


def _check_expected(label, view, expected):
    _expect(view['observations'] == expected['observations'], f'{label}: observations differ')
    _expect(view['positions'] == expected['positions'], f'{label}: original order differs')
    _expect(view['mm'] == expected['mm'], f'{label}: millimetres differ')
    _expect(view['total_mm'] == expected['total_mm'], f'{label}: total differs')
    _expect(view['missing_count'] == expected['missing_count'],
            f'{label}: missing count differs')


def run_case(case, directory, *, with_retire=False):
    source = open_database(directory / 'source.sqlite')
    try:
        old, old_release, new, payload = _prepare_source(source, case)
    finally:
        source.close()

    mirror_path = directory / 'mirror.sqlite'
    medium_path = directory / 'medium.sqlite'
    mirror = open_mirror(mirror_path)
    try:
        medium = FixedSlots(medium_path, len(case['old']['observations']))
        try:
            _expect(initialize_rollover(mirror, medium, old_release.part(case['partition'])) == old,
                    'initialize_rollover did not return the old receipt')
            pending = read_rollover(mirror, medium, new)
            _expect(pending.status == 'pending', 'new receipt must be pending before delivery')
            _check_expected('old-before-delivery', _view(read_rollover(mirror, medium, old)),
                            case['old'])
            first = receive_rollover(mirror, medium, payload)
            _expect(receive_rollover(mirror, medium, payload) == first,
                    'a replayed rollover package must return the first receipt')
            _check_expected('old', _view(read_rollover(mirror, medium, old)), case['old'])
            _check_expected('new', _view(read_rollover(mirror, medium, new)), case['new'])
        finally:
            medium.close()
    finally:
        mirror.close()

    # Close and reopen both stores, then confirm the account survives.
    mirror = open_mirror(mirror_path)
    try:
        medium = FixedSlots(medium_path, len(case['old']['observations']))
        try:
            _check_expected('reopen-old', _view(read_rollover(mirror, medium, old)), case['old'])
            _check_expected('reopen-new', _view(read_rollover(mirror, medium, new)), case['new'])
            if with_retire:
                result = retire_rollover(mirror, medium, 'retire-old', old)
                _expect(result == ('retire-old', old, new), 'retirement result differs')
                _expect(read_rollover(mirror, medium, old).status == 'retired',
                        'old receipt must be retired')
                _expect(medium.read(medium.spare) is None, 'the spare slot must be empty')
                _check_expected('retired-new', _view(read_rollover(mirror, medium, new)),
                                case['new'])
        finally:
            medium.close()
    finally:
        mirror.close()
    return {'case': case['case'], 'old_total': case['old']['total_mm'],
            'new_total': case['new']['total_mm']}


def check_interruptions(case, directory):
    """Drive every medium hook boundary and resume with the same arguments."""
    source = open_database(directory / 'source.sqlite')
    try:
        old, old_release, new, payload = _prepare_source(source, case)
    finally:
        source.close()
    slots = len(case['old']['observations'])
    observations = []

    accept_trips = [('replace', 'before', slots), ('replace', 'after', slots)] + \
        [('replace', 'after', index) for index in range(slots)]
    for trip in accept_trips:
        path = directory / f'cold-{trip[0]}-{trip[1]}-{trip[2]}'
        path.mkdir()
        mirror = open_mirror(path / 'mirror.sqlite')
        medium_path = path / 'medium.sqlite'
        try:
            medium = FixedSlots(medium_path, slots)
            initialize_rollover(mirror, medium, old_release.part('east'))
            medium.close()
            medium = FixedSlots(medium_path, slots, hook=_trip(trip))
            try:
                try:
                    receive_rollover(mirror, medium, payload)
                    observation = 'no-interruption'
                except _Trip:
                    observation = 'interrupted'
            finally:
                medium.close()
            mirror.close()
            mirror = open_mirror(path / 'mirror.sqlite')
            medium = FixedSlots(medium_path, slots)
            try:
                pending = read_rollover(mirror, medium, new)
                if observation == 'interrupted':
                    _expect(pending.status == 'pending',
                            'an interrupted acceptance must not report the new version ready')
                _check_expected('after-interrupt-old',
                                _view(read_rollover(mirror, medium, old)), case['old'])
                first = receive_rollover(mirror, medium, payload)
                _check_expected('resumed-new', _view(read_rollover(mirror, medium, new)),
                                case['new'])
                _expect(receive_rollover(mirror, medium, payload) == first,
                        'resumed acceptance must keep the first receipt')
            finally:
                medium.close()
                mirror.close()
        finally:
            try:
                mirror.close()
            except Exception:
                pass
        observations.append({'trip': list(trip), 'interrupted': observation})

    # Retirement interruption: the directory mark commits before the spare
    # slot is cleared, so the first 'retired' report is permanent.
    for trip in (('clear_spare', 'before', slots), ('clear_spare', 'after', slots)):
        path = directory / f'cold-retire-{trip[1]}'
        path.mkdir()
        mirror = open_mirror(path / 'mirror.sqlite')
        medium_path = path / 'medium.sqlite'
        try:
            medium = FixedSlots(medium_path, slots)
            initialize_rollover(mirror, medium, old_release.part('east'))
            receive_rollover(mirror, medium, payload)
            medium.close()
            medium = FixedSlots(medium_path, slots, hook=_trip(trip))
            try:
                try:
                    retire_rollover(mirror, medium, 'retire-old', old)
                    raise VerificationError('retirement should have been interrupted')
                except _Trip:
                    pass
                _expect(read_rollover(mirror, medium, old).status == 'retired',
                        'retirement must be reported before the spare slot is cleared')
            finally:
                medium.close()
            mirror.close()
            mirror = open_mirror(path / 'mirror.sqlite')
            medium = FixedSlots(medium_path, slots)
            try:
                _expect(retire_rollover(mirror, medium, 'retire-old', old)
                        == ('retire-old', old, new), 'resumed retirement result differs')
                _expect(medium.read(medium.spare) is None,
                        'resumed retirement must empty the spare slot')
                _expect(read_rollover(mirror, medium, old).status == 'retired',
                        'the old receipt stays retired after resuming')
                _check_expected('retired-final', _view(read_rollover(mirror, medium, new)),
                                case['new'])
            finally:
                medium.close()
                mirror.close()
        finally:
            try:
                mirror.close()
            except Exception:
                pass
        observations.append({'trip': list(trip), 'interrupted': 'interrupted'})
    return observations


class _Trip(Exception):
    pass


def _trip(target):
    def hook(action, phase, index):
        if (action, phase, index) == target:
            raise _Trip
    return hook


def main():
    summary = {'cases': [], 'interruptions': []}
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        for index, case in enumerate((_load_case(0), _load_case(1))):
            directory = root / f'case-{index}'
            directory.mkdir()
            summary['cases'].append(run_case(case, directory, with_retire=(index == 0)))
        directory = root / 'interrupt'
        directory.mkdir()
        summary['interruptions'] = check_interruptions(_load_case(0), directory)
    print('ROLLOVER_VERIFY ' + json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    main()
