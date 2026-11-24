from dataclasses import dataclass

from lake.database import atomic
from lake.ledger.revisions import (
    OfficialReceipt, OfficialRevision, OfficialRow, read_partition_changes,
    read_revision, read_revision_rows, resolve_official_receipt,
)
from lake.ledger.totals import Totals, calculate_totals
from lake.query.scope import normalize_partitions


class RevisionNotFound(LookupError):
    pass


@dataclass(frozen=True)
class SourceAccount:
    receipt: OfficialReceipt
    rows: tuple[OfficialRow, ...]
    totals: Totals


@dataclass(frozen=True)
class ReleaseBasis:
    revision: OfficialRevision
    rows: tuple[OfficialRow, ...]
    partition_changes: tuple[tuple[int, tuple[str, ...]], ...]


def source_read(db, revision, partitions):
    scope = normalize_partitions(partitions)
    members = set(scope)
    with atomic(db, write=False):
        official = read_revision(db, revision)
        if official is None:
            raise RevisionNotFound(revision)
        rows = tuple(row for row in read_revision_rows(db, revision) if row.partition in members)
    return SourceAccount(official.receipt, rows, calculate_totals(rows, scope))


def read_release_basis(db, receipt):
    official = resolve_official_receipt(db, receipt)
    if official is None:
        raise RevisionNotFound(receipt)
    number = official.receipt.revision
    return ReleaseBasis(official, read_revision_rows(db, number), read_partition_changes(db, number))
