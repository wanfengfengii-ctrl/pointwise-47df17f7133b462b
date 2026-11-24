from lake.database import atomic
from lake.intake.store import find_batch, read_accepted_rows
from lake.intake.validate import validate_name
from lake.ledger.revisions import (
    OfficialRevision, OfficialRow, current_revision, find_posted_receipt,
    next_receipt, read_revision_rows, save_revision,
)


class ReceiptNotFound(LookupError):
    pass


def post(db, batch_id):
    try:
        validate_name(batch_id, 'batch id')
    except ValueError as exc:
        raise ReceiptNotFound(batch_id) from exc
    with atomic(db):
        existing = find_posted_receipt(db, batch_id)
        if existing is not None:
            return existing
        if find_batch(db, batch_id) is None:
            raise ReceiptNotFound(batch_id)
        accepted = read_accepted_rows(db, batch_id)
        parent = current_revision(db)
        parent_number = None if parent is None else parent.receipt.revision
        previous = () if parent_number is None else read_revision_rows(db, parent_number)
        appended = tuple(OfficialRow(row.key, row.partition, row.source, row.line, row.mm,
                                     len(previous) + row.position) for row in accepted)
        receipt = next_receipt(db, parent)
        affected = tuple(sorted({row.partition for row in appended}))
        revision = OfficialRevision(receipt, parent_number, 'post', batch_id, affected)
        save_revision(db, revision, previous + appended, posted_batch=batch_id)
        return receipt
