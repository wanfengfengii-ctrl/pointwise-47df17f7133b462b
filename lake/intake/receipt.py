from dataclasses import dataclass


@dataclass(frozen=True)
class IntakeReceipt:
    batch_id: str
    row_count: int


class BatchRejected(ValueError):
    pass


class BatchConflict(BatchRejected):
    pass
