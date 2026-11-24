from dataclasses import dataclass

from lake.dispatch.manifest import ReleaseManifest
from lake.ledger.revisions import OfficialRow


# A transport projection preserves the official row without changing its identity.
PackageRow = OfficialRow


@dataclass(frozen=True)
class PartitionPackage:
    package_id: str
    release_id: str
    partition: str
    manifest: ReleaseManifest
    rows: tuple[PackageRow, ...]

    @property
    def checkpoint(self):
        return self.manifest.checkpoint(self.partition)


def release_identifier(receipt):
    return f'{receipt.lake_id}/{receipt.day}/{receipt.revision}'


def part_identifier(release_id, partition):
    return f'{release_id}/{partition}'


def build_partition_package(manifest, partition, rows):
    if manifest.checkpoint(partition) == 0:
        raise ValueError('partition is outside the release coverage')
    release_id = release_identifier(manifest.receipt)
    projection = tuple(row for row in rows if row.partition == partition)
    return PartitionPackage(part_identifier(release_id, partition), release_id,
                            partition, manifest, projection)
