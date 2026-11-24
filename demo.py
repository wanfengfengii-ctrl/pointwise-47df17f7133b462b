from contextlib import closing
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
from lake.ledger.history import source_read
from lake.ledger.revisions import initialize_ledger
from lake.query.read import mirror_read, read_rollover
from lake.receiving.accept import (
    initialize_rollover, receive, receive_rollover, retire_rollover,
)
from lake.receiving.slots import FixedSlots
from lake.receiving.store import open_mirror


class _Trip(Exception):
    pass


def _trip(target):
    def hook(action, phase, index):
        if (action, phase, index) == target:
            raise _Trip
    return hook


def run_demo(daily, transfer, directory):
    batch_id = daily['batch_id']
    with closing(open_database(directory / 'source.sqlite')) as source:
        initialize_intake(source)
        initialize_ledger(source, daily['lake_id'], daily['day'])
        initialize_catalog(source)
        submit(source, batch_id, daily['rows'])
        first = post(source, batch_id)
        source_account = source_read(source, first.revision, {'east', 'north'})
        original = publish(source, first)
        moved = correct(source, transfer['correction_id'], first.revision,
                        ObservationKey(batch_id, transfer['observation']),
                        partition=transfer['partition'], mm=transfer['mm'])
        amended = publish(source, moved)

    mirror_path = directory / 'mirror.sqlite'
    with closing(open_mirror(mirror_path)) as mirror:
        for release, partition in ((original, 'east'), (original, 'north'), (amended, 'west')):
            receive(mirror, release.part(partition))
        partial = mirror_read(mirror, moved, {'east', 'west'})
        north = mirror_read(mirror, first, {'north'})
        if partial.status != 'pending':
            total = getattr(getattr(partial, 'totals', None), 'total_mm', None)
            raise SystemExit(f'MIRROR_TRANSFER_PARTIAL expected=pending actual_total_mm={total}')
        actual = {'initial_mm': source_account.totals.total_mm,
                  'east_west': partial.status, 'north_mm': north.totals.total_mm}
        receive(mirror, amended.part('east'))
    with closing(open_mirror(mirror_path)) as mirror:
        complete = mirror_read(mirror, moved, {'west', 'east'})
        downstream = {
            'rows': [[row.key.observation, row.partition, row.mm, row.source, row.line]
                     for row in complete.rows],
            'by_partition': {item.partition: item.total_mm for item in complete.totals.by_partition},
            'total_mm': complete.totals.total_mm,
        }
    expected = {'initial_mm': 12, 'east_west': 'pending', 'north_mm': 5}
    downstream_expected = {'rows': [['R', 'west', 11, 'station-E/aug14', 4]],
                           'by_partition': {'east': 0, 'west': 11}, 'total_mm': 11}
    if actual != expected or downstream != downstream_expected:
        raise SystemExit(f'MIRROR_TRANSFER_ACCOUNT actual={actual!r} complete={downstream!r}')
    scenario = {
        'case_id': 'N8',
        'input': {'daily': daily, 'transfer': transfer,
                  'deliveries': [[1, 'east'], [1, 'north'], [2, 'west'], [2, 'east']]},
        'expected': expected,
        'actual': actual,
        'downstream_expected': downstream_expected,
        'downstream_actual': downstream,
        'code_paths': ['lake/ledger/corrections.py', 'lake/dispatch/publish.py',
                       'lake/receiving/accept.py', 'lake/query/read.py'],
        'oracle': '手算初账 7+5。更正中撤出东区 R 并转入西区 11，双区补齐前欠撤项，北区沿用 5。',
    }
    print('POINTWISE_SCENARIO ' + json.dumps(scenario, ensure_ascii=False, sort_keys=True))


def run_rollover_demo(case, directory):
    """First sample group: cut the source, double-read, retire with a restart."""
    source = open_database(directory / 'source.sqlite')
    initialize_intake(source)
    initialize_ledger(source, case['lake_id'], case['day'])
    initialize_catalog(source)
    submit(source, case['batch_id'], case['rows'])
    old = post(source, case['batch_id'])
    old_release = publish(source, old)
    change = case['change']
    new = correct(source, change['correction_id'], old.revision,
                  ObservationKey(case['batch_id'], change['observation']), mm=change['mm'])
    payload = publish_rollover(source, old, new, case['partition'])
    source.close()

    mirror_path = directory / 'cold.sqlite'
    medium_path = directory / 'cold-medium.sqlite'
    slots = len(case['old']['observations'])
    mirror = open_mirror(mirror_path)
    medium = FixedSlots(medium_path, slots)
    initialize_rollover(mirror, medium, old_release.part(case['partition']))
    before = read_rollover(mirror, medium, new)
    old_before = read_rollover(mirror, medium, old)
    first = receive_rollover(mirror, medium, payload)
    replay = receive_rollover(mirror, medium, payload)
    old_ready = read_rollover(mirror, medium, old)
    new_ready = read_rollover(mirror, medium, new)
    medium.close()
    mirror.close()

    # Reopen both stores, then interrupt the retirement at the spare-slot clear.
    mirror = open_mirror(mirror_path)
    medium = FixedSlots(medium_path, slots, hook=_trip(('clear_spare', 'after', slots)))
    try:
        retire_rollover(mirror, medium, 'retire-old', old)
        interrupted = False
    except _Trip:
        interrupted = True
    retired_midway = read_rollover(mirror, medium, old).status
    medium.close()
    mirror.close()
    mirror = open_mirror(mirror_path)
    medium = FixedSlots(medium_path, slots)
    resumed = retire_rollover(mirror, medium, 'retire-old', old)
    old_final = read_rollover(mirror, medium, old)
    new_final = read_rollover(mirror, medium, new)
    spare = medium.read(medium.spare)
    medium.close()
    mirror.close()

    report = {
        'case_id': 'ROLLOVER-1',
        'receipts': {'old': old.revision, 'new': new.revision},
        'before_delivery': {'new': before.status,
                            'old_mm': old_before.totals.total_mm,
                            'old_missing': old_before.totals.missing_count},
        'delivered': {'receiving_id': first.receiving_id, 'package_id': first.package_id,
                      'replay_same': replay == first},
        'both_versions': {
            'old': {'mm': [row.mm for row in old_ready.rows],
                    'positions': [row.position for row in old_ready.rows],
                    'total_mm': old_ready.totals.total_mm,
                    'missing_count': old_ready.totals.missing_count},
            'new': {'mm': [row.mm for row in new_ready.rows],
                    'positions': [row.position for row in new_ready.rows],
                    'total_mm': new_ready.totals.total_mm,
                    'missing_count': new_ready.totals.missing_count,
                    'sources': [row.source for row in new_ready.rows],
                    'lines': [row.line for row in new_ready.rows]},
        },
        'retirement': {'interrupted': interrupted, 'old_midway': retired_midway,
                       'resumed': [resumed[0], resumed[1].revision, resumed[2].revision],
                       'old_final': old_final.status, 'new_final_mm': new_final.totals.total_mm,
                       'spare_empty': spare is None},
        'code_paths': ['lake/dispatch/rollover_wire.py', 'lake/dispatch/publish.py',
                       'lake/receiving/rollover.py', 'lake/query/read.py'],
        'oracle': '旧账 R7 S7 T缺，合计 14，缺测 1；新版只把 R 改为 11，合计 18，缺测 1。'
                  '断源后新版先报 pending，旧版完整可领；实收后两版分别报 14 和 18。'
                  '退旧中断于清空周转槽之后，重开续办成功，旧版报 retired，周转槽为空。',
    }
    scenario = {
        'case_id': 'ROLLOVER-1',
        'input': case,
        'expected': {'before_delivery': 'pending', 'old_total': case['old']['total_mm'],
                     'new_total': case['new']['total_mm'],
                     'retirement': 'retired', 'spare_empty': True},
        'actual': report,
    }
    expected_pairs = (('old_total', old_ready.totals.total_mm, case['old']['total_mm']),
                      ('new_total', new_ready.totals.total_mm, case['new']['total_mm']))
    for label, actual, expected in expected_pairs:
        if actual != expected:
            raise SystemExit(f'ROLLOVER_DEMO {label} expected={expected} actual={actual}')
    if before.status != 'pending' or old_before.totals.total_mm != case['old']['total_mm']:
        raise SystemExit('ROLLOVER_DEMO before-delivery account differs')
    if [row.mm for row in new_ready.rows] != case['new']['mm'] \
            or [row.position for row in new_ready.rows] != case['new']['positions']:
        raise SystemExit('ROLLOVER_DEMO new page differs')
    if not interrupted or retired_midway != 'retired' or old_final.status != 'retired' \
            or spare is not None or new_final.totals.total_mm != case['new']['total_mm']:
        raise SystemExit('ROLLOVER_DEMO retirement did not settle')
    print('POINTWISE_SCENARIO ' + json.dumps(scenario, ensure_ascii=False, sort_keys=True))
    return scenario


def main():
    samples = Path(__file__).resolve().parent / 'samples'
    daily = json.loads((samples / 'daily.json').read_text(encoding='utf-8'))
    transfer = json.loads((samples / 'transfer.json').read_text(encoding='utf-8'))
    rollover = json.loads((samples / 'rollover.json').read_text(encoding='utf-8'))[0]
    with TemporaryDirectory() as path:
        directory = Path(path)
        run_demo(daily, transfer, directory)
        rollover_directory = directory / 'rollover'
        rollover_directory.mkdir()
        run_rollover_demo(rollover, rollover_directory)


if __name__ == '__main__':
    main()
