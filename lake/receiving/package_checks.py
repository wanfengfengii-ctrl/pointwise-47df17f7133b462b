from lake.dispatch.partition_package import part_identifier, release_identifier
from lake.dispatch.wire import decode_package
from lake.intake.validate import (
    validate_mm, validate_name, validate_positive_id, validate_source,
)
from lake.ledger.revisions import validate_official_receipt
from lake.receiving.receipt import PackageRejected


def _check_manifest(package):
    receipt = validate_official_receipt(package.manifest.receipt)
    partition = validate_name(package.partition, 'package partition')
    release_id = release_identifier(receipt)
    if package.release_id != release_id or package.package_id != part_identifier(release_id, partition):
        raise ValueError('package identity does not match its declared receipt and partition')
    previous = ''
    for entry in package.manifest.coverage:
        name = validate_name(entry.partition, 'covered partition')
        version = validate_positive_id(entry.revision, 'partition checkpoint')
        if name <= previous or version > receipt.revision:
            raise ValueError('invalid partition coverage')
        previous = name
    if package.checkpoint == 0:
        raise ValueError('missing coverage for package partition')


def _check_rows(package):
    previous_position = 0
    keys = set()
    for row in package.rows:
        validate_name(row.key.batch_id, 'row batch')
        validate_name(row.key.observation, 'row observation')
        validate_source(row.source)
        validate_positive_id(row.line, 'source line')
        validate_mm(row.mm)
        position = validate_positive_id(row.position, 'formal row position')
        if row.partition != package.partition or position <= previous_position or row.key in keys:
            raise ValueError('inconsistent partition, order or observation identity')
        previous_position = position
        keys.add(row.key)


def check_package(payload):
    try:
        package = decode_package(payload)
        _check_manifest(package)
        _check_rows(package)
        return package
    except ValueError as exc:
        raise PackageRejected(str(exc)) from exc
