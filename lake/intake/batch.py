from lake.database import atomic
from lake.intake.receipt import BatchConflict, IntakeReceipt
from lake.intake.source_rows import ordered_batch_content
from lake.intake.store import find_batch, read_accepted_rows, save_batch
from lake.intake.validate import validate_batch


def submit(db, batch_id, observations):
    rows = ordered_batch_content(validate_batch(batch_id, observations))
    with atomic(db):
        receipt = find_batch(db, batch_id)
        if receipt is not None:
            if ordered_batch_content(read_accepted_rows(db, batch_id)) != rows:
                raise BatchConflict(f'batch {batch_id} has different accepted content')
            return receipt
        receipt = IntakeReceipt(batch_id, len(rows))
        save_batch(db, receipt, rows)
        return receipt
