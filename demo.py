from contextlib import closing
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from lake.database import open_database
from lake.dispatch.catalog import initialize_catalog
from lake.dispatch.publish import publish
from lake.intake.batch import submit
from lake.intake.source_rows import ObservationKey
from lake.intake.store import initialize_intake
from lake.ledger.commit import post
from lake.ledger.corrections import correct
from lake.ledger.history import source_read
from lake.ledger.revisions import initialize_ledger
from lake.query.read import mirror_read
from lake.receiving.accept import receive
from lake.receiving.store import open_mirror


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


def main():
    samples = Path(__file__).resolve().parent / 'samples'
    daily = json.loads((samples / 'daily.json').read_text(encoding='utf-8'))
    transfer = json.loads((samples / 'transfer.json').read_text(encoding='utf-8'))
    with TemporaryDirectory() as path:
        run_demo(daily, transfer, Path(path))


if __name__ == '__main__':
    main()
