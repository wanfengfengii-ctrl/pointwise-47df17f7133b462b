from dataclasses import dataclass
from typing import ClassVar

from lake.ledger.revisions import OfficialReceipt, OfficialRow
from lake.ledger.totals import Totals, calculate_totals
from lake.query.scope import MirrorRequest


@dataclass(frozen=True)
class MirrorAccount:
    requested_receipt: OfficialReceipt
    fulfilled_receipt: OfficialReceipt
    rows: tuple[OfficialRow, ...]
    totals: Totals
    status: ClassVar[str] = 'ready'


def reconcile_account(request: MirrorRequest, fulfilled_receipt, rows):
    return MirrorAccount(request.receipt, fulfilled_receipt, rows,
                         calculate_totals(rows, request.partitions))
