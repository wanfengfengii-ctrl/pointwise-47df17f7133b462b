from collections.abc import Sequence, Set
from dataclasses import dataclass

from lake.intake.validate import validate_name
from lake.ledger.revisions import OfficialReceipt, validate_official_receipt


@dataclass(frozen=True)
class MirrorRequest:
    receipt: OfficialReceipt
    partitions: tuple[str, ...]


class InvalidMirrorRequest(ValueError):
    pass


def normalize_partitions(partitions):
    if not isinstance(partitions, (Sequence, Set)) or isinstance(partitions, (str, bytes, bytearray)):
        raise ValueError('expected a nonempty partition sequence or set')
    names = {validate_name(name, 'requested partition') for name in partitions}
    if not names:
        raise ValueError('empty partition scope')
    return tuple(sorted(names))


def normalize_request(receipt, partitions):
    try:
        return MirrorRequest(validate_official_receipt(receipt), normalize_partitions(partitions))
    except ValueError as exc:
        raise InvalidMirrorRequest(str(exc)) from exc
