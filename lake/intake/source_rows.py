from dataclasses import dataclass


@dataclass(frozen=True)
class ObservationKey:
    batch_id: str
    observation: str


@dataclass(frozen=True)
class SourceRow:
    key: ObservationKey
    partition: str
    source: str
    line: int
    mm: int | None
    position: int


def ordered_batch_content(rows):
    return tuple(sorted(rows, key=lambda row: row.position))


def encode_mm(value):
    # Store exact values outside SQLite's signed 64-bit integer representation.
    return None if value is None else hex(value)


def decode_mm(value):
    if value is None:
        return None
    if type(value) is not str:
        raise ValueError('millimetres must be canonical hexadecimal text or null')
    number = int(value, 16)
    if hex(number) != value:
        raise ValueError('noncanonical millimetres')
    return number
