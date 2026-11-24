from collections.abc import Mapping, Sequence
from datetime import date
import re

from lake.intake.receipt import BatchRejected
from lake.intake.source_rows import ObservationKey, SourceRow


_NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}')
_FIELDS = {'observation', 'partition', 'source', 'line', 'unit', 'mm'}
_MAX_SQLITE_ID = (1 << 63) - 1


def validate_name(value, label):
    if type(value) is not str or not _NAME.fullmatch(value):
        raise ValueError(f'invalid {label}')
    return value


def validate_day(value):
    if type(value) is not str or date.fromisoformat(value).isoformat() != value:
        raise ValueError('invalid observation day')
    return value


def validate_positive_id(value, label):
    if type(value) is not int or not 1 <= value <= _MAX_SQLITE_ID:
        raise ValueError(f'invalid {label}')
    return value


def validate_source(value):
    if type(value) is not str or not value.strip() or not value.isprintable():
        raise ValueError('invalid source locator')
    return value


def validate_mm(value):
    if value is not None and type(value) is not int:
        raise ValueError('expected integer millimetres or explicit missing value')
    return value


def validate_observation(batch_id, position, data):
    try:
        validate_name(batch_id, 'batch id')
        validate_positive_id(position, 'source position')
        if not isinstance(data, Mapping) or data.keys() != _FIELDS:
            raise ValueError('expected observation, partition, source, line, unit and mm')
        if data['unit'] != 'mm':
            raise ValueError('unit must be mm')
        return SourceRow(
            ObservationKey(batch_id, validate_name(data['observation'], 'observation')),
            validate_name(data['partition'], 'partition'),
            validate_source(data['source']),
            validate_positive_id(data['line'], 'source line'),
            validate_mm(data['mm']),
            position,
        )
    except ValueError as exc:
        raise BatchRejected(f'row {position}: {exc}') from exc


def validate_batch(batch_id, observations):
    try:
        validate_name(batch_id, 'batch id')
    except ValueError as exc:
        raise BatchRejected(str(exc)) from exc
    if not isinstance(observations, Sequence) or isinstance(observations, (str, bytes, bytearray)):
        raise BatchRejected('expected an ordered batch sequence')
    if not observations:
        raise BatchRejected('empty batch')
    rows = tuple(validate_observation(batch_id, pos, item)
                 for pos, item in enumerate(observations, 1))
    if len({row.key for row in rows}) != len(rows):
        raise BatchRejected('duplicate observation within batch')
    return rows
