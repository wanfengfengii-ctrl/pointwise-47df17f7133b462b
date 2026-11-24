from dataclasses import dataclass

from lake.dispatch.manifest import ReleaseManifest
from lake.dispatch.partition_package import PartitionPackage
from lake.dispatch.wire import decode_package
from lake.receiving.receipt import PackageConflict, ReceivingReceipt


@dataclass(frozen=True)
class AcceptedPackage:
    receipt: ReceivingReceipt
    payload: bytes


@dataclass(frozen=True)
class ReceivedEvidence:
    manifests: tuple[ReleaseManifest, ...]
    projections: dict[tuple[str, int], PartitionPackage]


def find_package(db, package_id):
    record = db.execute('''SELECT receiving_id, payload
        FROM mirror_packages WHERE package_id = ?''', (package_id,)).fetchone()
    if record is None:
        return None
    return AcceptedPackage(ReceivingReceipt(record[0], package_id), record[1])


def record_package(db, package, payload):
    formal = package.manifest.receipt
    projection_key = (formal.lake_id, formal.day, package.partition, package.checkpoint)
    existing = db.execute('''SELECT p.payload FROM mirror_projections AS a
        JOIN mirror_packages AS p USING (receiving_id)
        WHERE a.lake_id = ? AND a.day = ? AND a.partition = ? AND a.checkpoint = ?''',
                          projection_key).fetchone()
    if existing is not None and decode_package(existing[0]).rows != package.rows:
        raise PackageConflict('different rows for an already received partition checkpoint')

    insert = db.execute('''INSERT INTO mirror_packages
        (package_id, lake_id, day, revision, payload) VALUES (?, ?, ?, ?, ?)''',
                        (package.package_id, formal.lake_id, formal.day, formal.revision, payload))
    receiving_id = insert.lastrowid
    if existing is None:
        db.execute('''INSERT INTO mirror_projections
            (lake_id, day, partition, checkpoint, receiving_id) VALUES (?, ?, ?, ?, ?)''',
                   (*projection_key, receiving_id))
    return ReceivingReceipt(receiving_id, package.package_id)


def read_received_evidence(db, receipt):
    book = (receipt.lake_id, receipt.day)
    manifest_records = db.execute('''SELECT payload FROM mirror_packages
        WHERE lake_id = ? AND day = ? AND revision >= ? ORDER BY revision, package_id''',
                                  (*book, receipt.revision))
    manifests = tuple(decode_package(payload).manifest for (payload,) in manifest_records)
    projection_records = db.execute('''SELECT a.partition, a.checkpoint, p.payload
        FROM mirror_projections AS a JOIN mirror_packages AS p USING (receiving_id)
        WHERE a.lake_id = ? AND a.day = ?''', book)
    projections = {(partition, checkpoint): decode_package(payload)
                   for partition, checkpoint, payload in projection_records}
    return ReceivedEvidence(manifests, projections)
