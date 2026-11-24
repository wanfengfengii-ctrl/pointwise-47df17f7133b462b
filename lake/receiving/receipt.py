from dataclasses import dataclass


@dataclass(frozen=True)
class ReceivingReceipt:
    receiving_id: int
    package_id: str


class PackageRejected(ValueError):
    pass


class PackageConflict(PackageRejected):
    pass
