from lake.database import atomic
from lake.dispatch.catalog import (
    ReleaseRecord, find_release, find_rollover_release, save_release, save_rollover_release,
)
from lake.dispatch.manifest import build_manifest
from lake.dispatch.partition_package import build_partition_package, release_identifier
from lake.dispatch.rollover_wire import (
    RolloverOrderEntry, RolloverPackage, RolloverTarget, encode_rollover, rollover_package_id,
    row_digest, rows_digest,
)
from lake.dispatch.wire import encode_package
from lake.intake.validate import validate_name
from lake.ledger.history import read_release_basis
from lake.ledger.revisions import (
    read_revision, read_revision_rows, resolve_official_receipt, validate_official_receipt,
)


class PublicationRejected(ValueError):
    pass


def publish(db, receipt):
    with atomic(db):
        if resolve_official_receipt(db, receipt) is None:
            raise PublicationRejected('unknown official receipt for this observation book')
        stored = find_release(db, receipt)
        if stored is not None:
            return stored
        basis = read_release_basis(db, receipt)
        manifest = build_manifest(basis)
        parts = []
        for entry in manifest.coverage:
            package = build_partition_package(manifest, entry.partition, basis.rows)
            parts.append((entry.partition, encode_package(package)))
        release = ReleaseRecord(release_identifier(receipt), receipt, tuple(parts))
        save_release(db, release)
        return release


def _projection(rows, partition):
    return tuple(row for row in rows if row.partition == partition)


def _rollover_contents(db, old, new, partition):
    old_official = read_revision(db, old.revision)
    new_official = read_revision(db, new.revision)
    if old_official is None or new_official is None:
        raise PublicationRejected('unknown official receipt for this observation book')
    if new_official.parent != old.revision:
        raise PublicationRejected('new receipt is not the direct child of the old receipt')
    if new_official.operation_kind != 'correction':
        raise PublicationRejected('new receipt does not record a correction')
    old_rows = _projection(read_revision_rows(db, old.revision), partition)
    new_rows = _projection(read_revision_rows(db, new.revision), partition)
    if not old_rows:
        raise PublicationRejected('partition has no official row at the old receipt')
    if len(old_rows) != len(new_rows):
        raise PublicationRejected('row count changed in the rollover partition')
    # Every row outside the rollover partition must be untouched.
    outside_old = tuple(row for row in read_revision_rows(db, old.revision)
                        if row.partition != partition)
    outside_new = tuple(row for row in read_revision_rows(db, new.revision)
                        if row.partition != partition)
    if outside_old != outside_new:
        raise PublicationRejected('rows outside the rollover partition changed')
    changed = []
    for before, after in zip(old_rows, new_rows):
        if (before.key, before.partition, before.source, before.line, before.position) != \
                (after.key, after.partition, after.source, after.line, after.position):
            raise PublicationRejected('rollover partition rows were re-keyed, moved or reordered')
        if before.mm != after.mm:
            changed.append((before, after))
    if len(changed) != 1:
        raise PublicationRejected('rollover partition must contain exactly one replaced value')
    before, after = changed[0]
    order = tuple(RolloverOrderEntry(row.key.batch_id, row.key.observation, row.position)
                  for row in old_rows)
    index = old_rows.index(before)
    parent_digest = row_digest(old_rows[index - 1]) if index > 0 else None
    child_digest = row_digest(old_rows[index + 1]) if index + 1 < len(old_rows) else None
    target = RolloverTarget(after.key.batch_id, after.key.observation, after.position,
                            row_digest(before), parent_digest, child_digest)
    return order, target, after, rows_digest(old_rows), rows_digest(new_rows)


def publish_rollover(db, old, new, partition):
    try:
        validate_official_receipt(old)
        validate_official_receipt(new)
        validate_name(partition, 'rollover partition')
        if (old.lake_id, old.day) != (new.lake_id, new.day):
            raise ValueError('both receipts must belong to the same observation book')
    except ValueError as exc:
        raise PublicationRejected(str(exc)) from exc
    with atomic(db):
        for receipt in (old, new):
            if resolve_official_receipt(db, receipt) is None:
                raise PublicationRejected('unknown official receipt for this observation book')
        stored = find_rollover_release(db, old.lake_id, old.day, partition,
                                       old.revision, new.revision)
        if stored is not None:
            return stored
        order, target, row, old_digest, new_digest = _rollover_contents(db, old, new, partition)
        package = RolloverPackage(rollover_package_id(old, new, partition), partition,
                                  old, new, order, old_digest, new_digest, target, row)
        payload = encode_rollover(package)
        save_rollover_release(db, old.lake_id, old.day, partition,
                              old.revision, new.revision, payload)
        return payload
