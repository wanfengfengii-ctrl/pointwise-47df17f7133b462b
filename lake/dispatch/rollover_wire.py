"""Transport encoding for a cold-mirror rollover package.

A rollover package freezes the two official receipts that bracket one
in-place value replacement.  It carries only the body of the new replacement
row; everything else is a credential: the two receipts, the original order of
the old partition, a digest of both full projections and the target row's
identity with its old body digest and its neighbours' digests.
"""
from dataclasses import dataclass
import hashlib
import json

from lake.intake.source_rows import ObservationKey, decode_mm, encode_mm
from lake.ledger.revisions import OfficialReceipt, OfficialRow


class RolloverFormatError(ValueError):
    pass


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True, allow_nan=False).encode('ascii')


def _sha256(value):
    return hashlib.sha256(value).hexdigest()


def _receipt_dict(receipt):
    return {'lake_id': receipt.lake_id, 'day': receipt.day, 'revision': receipt.revision}


def row_dict(row):
    return {
        'batch_id': row.key.batch_id,
        'observation': row.key.observation,
        'partition': row.partition,
        'source': row.source,
        'line': row.line,
        'position': row.position,
        'mm': encode_mm(row.mm),
    }


def row_digest(row):
    return _sha256(_canonical(row_dict(row)))


def rows_digest(rows):
    return _sha256(_canonical([row_dict(row) for row in rows]))


def digests_digest(digests):
    """Digest of a projection expressed by its per-row body digests."""
    return _sha256(_canonical(list(digests)))


@dataclass(frozen=True)
class RolloverOrderEntry:
    batch_id: str
    observation: str
    position: int

    @property
    def key(self):
        return ObservationKey(self.batch_id, self.observation)


@dataclass(frozen=True)
class RolloverTarget:
    batch_id: str
    observation: str
    position: int
    old_digest: str
    before_digest: str | None
    after_digest: str | None

    @property
    def key(self):
        return ObservationKey(self.batch_id, self.observation)


@dataclass(frozen=True)
class RolloverPackage:
    package_id: str
    partition: str
    old_receipt: OfficialReceipt
    new_receipt: OfficialReceipt
    old_order: tuple[RolloverOrderEntry, ...]
    old_digest: str
    new_digest: str
    target: RolloverTarget
    row: OfficialRow


def rollover_package_id(old, new, partition):
    material = _canonical({'lake_id': old.lake_id, 'day': old.day,
                           'old': old.revision, 'new': new.revision,
                           'partition': partition})
    return f'roll-{old.revision}-{new.revision}-{_sha256(material)[:24]}'


def _body(package):
    return {
        'kind': 'rollover',
        'package_id': package.package_id,
        'partition': package.partition,
        'old_receipt': _receipt_dict(package.old_receipt),
        'new_receipt': _receipt_dict(package.new_receipt),
        'old_order': [
            {'batch_id': entry.batch_id, 'observation': entry.observation,
             'position': entry.position}
            for entry in package.old_order
        ],
        'old_digest': package.old_digest,
        'new_digest': package.new_digest,
        'target': {'batch_id': package.target.batch_id,
                   'observation': package.target.observation,
                   'position': package.target.position,
                   'old_digest': package.target.old_digest,
                   'before_digest': package.target.before_digest,
                   'after_digest': package.target.after_digest},
        'row': row_dict(package.row),
    }


def encode_rollover(package):
    body = _body(package)
    return _canonical({'version': 1, 'sha256': _sha256(_canonical(body)), 'body': body})


def sealed_digest(package):
    return _sha256(encode_rollover(package))


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RolloverFormatError(f'duplicate object key: {key}')
        result[key] = value
    return result


def _fields(value, names):
    if type(value) is not dict or value.keys() != names:
        raise RolloverFormatError('unexpected object fields')
    return value


def _array(value):
    if type(value) is not list:
        raise RolloverFormatError('expected an array')
    return value


def _text(value, label):
    if type(value) is not str:
        raise RolloverFormatError(f'expected text for {label}')
    return value


def _optional_text(value, label):
    if value is not None and type(value) is not str:
        raise RolloverFormatError(f'expected optional text for {label}')
    return value


def _revision(value, label):
    if type(value) is not int or value < 1:
        raise RolloverFormatError(f'expected a positive integer for {label}')
    return value


def _decode_receipt(data):
    _fields(data, {'lake_id', 'day', 'revision'})
    return OfficialReceipt(_text(data['lake_id'], 'lake id'), _text(data['day'], 'day'),
                           _revision(data['revision'], 'revision'))


def _decode_row(data):
    _fields(data, {'batch_id', 'observation', 'partition', 'source', 'line', 'position', 'mm'})
    return OfficialRow(ObservationKey(_text(data['batch_id'], 'batch'),
                                      _text(data['observation'], 'observation')),
                       _text(data['partition'], 'partition'),
                       _text(data['source'], 'source'),
                       data['line'],
                       decode_mm(data['mm']),
                       data['position'])


def decode_rollover(payload):
    if type(payload) is not bytes:
        raise RolloverFormatError('a rollover package must be transported as bytes')
    try:
        envelope = json.loads(payload.decode('utf-8'), object_pairs_hook=_unique_object)
        _fields(envelope, {'version', 'sha256', 'body'})
        if type(envelope['version']) is not int or envelope['version'] != 1:
            raise RolloverFormatError('unsupported rollover package version')
        body = _fields(envelope['body'], {'kind', 'package_id', 'partition', 'old_receipt',
                                        'new_receipt', 'old_order', 'old_digest',
                                        'new_digest', 'target', 'row'})
        if body['kind'] != 'rollover':
            raise RolloverFormatError('not a rollover package')
        if envelope['sha256'] != _sha256(_canonical(body)):
            raise RolloverFormatError('rollover body digest mismatch')
        old_order = []
        for raw in _array(body['old_order']):
            _fields(raw, {'batch_id', 'observation', 'position'})
            old_order.append(RolloverOrderEntry(_text(raw['batch_id'], 'order batch'),
                                                _text(raw['observation'], 'order observation'),
                                                _revision(raw['position'], 'order position')))
        target_data = _fields(body['target'], {'batch_id', 'observation', 'position',
                                               'old_digest', 'before_digest', 'after_digest'})
        target = RolloverTarget(_text(target_data['batch_id'], 'target batch'),
                                _text(target_data['observation'], 'target observation'),
                                _revision(target_data['position'], 'target position'),
                                _text(target_data['old_digest'], 'target digest'),
                                _optional_text(target_data['before_digest'], 'before digest'),
                                _optional_text(target_data['after_digest'], 'after digest'))
        return RolloverPackage(
            _text(body['package_id'], 'package id'),
            _text(body['partition'], 'partition'),
            _decode_receipt(body['old_receipt']),
            _decode_receipt(body['new_receipt']),
            tuple(old_order),
            _text(body['old_digest'], 'old digest'),
            _text(body['new_digest'], 'new digest'),
            target,
            _decode_row(body['row']),
        )
    except RolloverFormatError:
        raise
    except (ValueError, TypeError, RecursionError, KeyError) as exc:
        raise RolloverFormatError('invalid rollover package encoding') from exc
