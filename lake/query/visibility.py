from collections import defaultdict
from dataclasses import dataclass
from typing import ClassVar

from lake.dispatch.manifest import ReleaseManifest
from lake.dispatch.partition_package import PartitionPackage
from lake.ledger.revisions import OfficialReceipt


@dataclass(frozen=True)
class VisibleSelection:
    manifest: ReleaseManifest
    packages: tuple[PartitionPackage, ...]


@dataclass(frozen=True)
class PendingMaterials:
    requested_receipt: OfficialReceipt
    partitions: tuple[str, ...]
    status: ClassVar[str] = 'pending'


def select_visible_materials(request, evidence):
    candidates = defaultdict(set)
    for manifest in evidence.manifests:
        candidates[manifest.receipt.revision].add(manifest)

    for number in sorted(candidates):
        claims = candidates[number]
        if number < request.receipt.revision or len(claims) != 1:
            continue
        manifest = next(iter(claims))
        checkpoints = {entry.partition: entry.revision for entry in manifest.coverage}
        packages = []
        for partition in request.partitions:
            checkpoint = checkpoints.get(partition, 0)
            if checkpoint == 0:
                continue
            package = evidence.projections.get((partition, checkpoint))
            if package is None:
                break
            packages.append(package)
        else:
            return VisibleSelection(manifest, tuple(packages))
    return PendingMaterials(request.receipt, request.partitions)
