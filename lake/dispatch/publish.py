from lake.database import atomic
from lake.dispatch.catalog import ReleaseRecord, find_release, save_release
from lake.dispatch.manifest import build_manifest
from lake.dispatch.partition_package import build_partition_package, release_identifier
from lake.dispatch.wire import encode_package
from lake.ledger.history import read_release_basis
from lake.ledger.revisions import resolve_official_receipt


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
