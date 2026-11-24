from dataclasses import dataclass

from lake.ledger.revisions import OfficialReceipt


@dataclass(frozen=True, order=True)
class PartitionCoverage:
    partition: str
    revision: int


@dataclass(frozen=True)
class ReleaseManifest:
    receipt: OfficialReceipt
    coverage: tuple[PartitionCoverage, ...]

    def checkpoint(self, partition):
        for entry in self.coverage:
            if entry.partition == partition:
                return entry.revision
        return 0

    @property
    def affected(self):
        return tuple(entry.partition for entry in self.coverage
                     if entry.revision == self.receipt.revision)


def build_manifest(basis):
    checkpoints = {}
    for number, affected in basis.partition_changes:
        for partition in affected:
            checkpoints[partition] = number
    coverage = tuple(PartitionCoverage(name, number) for name, number in sorted(checkpoints.items()))
    return ReleaseManifest(basis.revision.receipt, coverage)
