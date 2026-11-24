from dataclasses import replace

from lake.database import atomic
from lake.intake.source_rows import ObservationKey
from lake.intake.validate import validate_mm, validate_name, validate_positive_id
from lake.ledger.revisions import (
    OfficialRevision, correction_exists, current_revision, next_receipt,
    read_revision_rows, save_revision,
)


class CorrectionRejected(ValueError):
    pass


_UNCHANGED = object()


def correct(db, correction_id, parent_revision, target, *, partition=None, mm=_UNCHANGED):
    try:
        validate_name(correction_id, 'correction id')
        validate_positive_id(parent_revision, 'parent revision')
        if type(target) is not ObservationKey:
            raise ValueError('expected an observation key')
        validate_name(target.batch_id, 'target batch')
        validate_name(target.observation, 'target observation')
        if partition is not None:
            validate_name(partition, 'partition')
        if mm is not _UNCHANGED:
            validate_mm(mm)
        if partition is None and mm is _UNCHANGED:
            raise ValueError('supply a new partition or value')
    except ValueError as exc:
        raise CorrectionRejected(str(exc)) from exc

    with atomic(db):
        parent = current_revision(db)
        if parent is None or parent.receipt.revision != parent_revision:
            raise CorrectionRejected('parent is not the current formal revision')
        if correction_exists(db, correction_id):
            raise CorrectionRejected('correction id is already in use')
        rows = list(read_revision_rows(db, parent_revision))
        for index, row in enumerate(rows):
            if row.key == target:
                updated = replace(row, partition=row.partition if partition is None else partition,
                                  mm=row.mm if mm is _UNCHANGED else mm)
                rows[index] = updated
                affected = tuple(sorted({row.partition, updated.partition}))
                receipt = next_receipt(db, parent)
                revision = OfficialRevision(receipt, parent_revision, 'correction',
                                            correction_id, affected)
                save_revision(db, revision, rows)
                return receipt
        raise CorrectionRejected('target observation does not exist at parent revision')
