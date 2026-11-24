from lake.dispatch.wire import encode_package
from lake.receiving.inventory import find_package, record_package
from lake.receiving.package_checks import check_package
from lake.receiving.receipt import PackageConflict
from lake.receiving.rollover import (
    initialize_rollover, receive_rollover, retire_rollover,
)
from lake.receiving.store import receiving_transaction

__all__ = [
    'receive',
    'initialize_rollover',
    'receive_rollover',
    'retire_rollover',
]


def receive(db, payload):
    package = check_package(payload)
    sealed = encode_package(package)
    with receiving_transaction(db):
        stored = find_package(db, package.package_id)
        if stored is not None:
            if stored.payload != sealed:
                raise PackageConflict('a different package already has this identity')
            return stored.receipt
        return record_package(db, package, sealed)
