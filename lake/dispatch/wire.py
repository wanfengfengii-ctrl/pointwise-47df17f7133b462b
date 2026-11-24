import hashlib
import json

from lake.dispatch.manifest import PartitionCoverage, ReleaseManifest
from lake.dispatch.partition_package import PackageRow, PartitionPackage
from lake.intake.source_rows import ObservationKey, decode_mm, encode_mm
from lake.ledger.revisions import OfficialReceipt


class PackageFormatError(ValueError):
    pass


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True, allow_nan=False).encode('ascii')


def package_digest(body):
    return hashlib.sha256(_canonical(body)).hexdigest()


def encode_package(package):
    receipt = package.manifest.receipt
    body = {
        'package_id': package.package_id,
        'release_id': package.release_id,
        'partition': package.partition,
        'receipt': {'lake_id': receipt.lake_id, 'day': receipt.day, 'revision': receipt.revision},
        'coverage': [{'partition': entry.partition, 'revision': entry.revision}
                     for entry in package.manifest.coverage],
        'rows': [
            {'batch_id': row.key.batch_id, 'observation': row.key.observation,
             'partition': row.partition, 'source': row.source, 'line': row.line,
             'position': row.position, 'mm': encode_mm(row.mm)}
            for row in package.rows
        ],
    }
    return _canonical({'version': 1, 'sha256': package_digest(body), 'body': body})


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise PackageFormatError(f'duplicate object key: {key}')
        result[key] = value
    return result


def _fields(value, names):
    if type(value) is not dict or value.keys() != names:
        raise PackageFormatError('unexpected object fields')
    return value


def _array(value):
    if type(value) is not list:
        raise PackageFormatError('expected an array')
    return value


def _decode_row(data):
    _fields(data, {'batch_id', 'observation', 'partition', 'source', 'line', 'position', 'mm'})
    return PackageRow(ObservationKey(data['batch_id'], data['observation']), data['partition'],
                      data['source'], data['line'], decode_mm(data['mm']), data['position'])


def decode_package(payload):
    if type(payload) is not bytes:
        raise PackageFormatError('a package must be transported as bytes')
    try:
        envelope = json.loads(payload.decode('utf-8'), object_pairs_hook=_unique_object)
        _fields(envelope, {'version', 'sha256', 'body'})
        if type(envelope['version']) is not int or envelope['version'] != 1:
            raise PackageFormatError('unsupported package version')
        body = _fields(envelope['body'], {'package_id', 'release_id', 'partition', 'receipt',
                                         'coverage', 'rows'})
        if envelope['sha256'] != package_digest(body):
            raise PackageFormatError('package body digest mismatch')
        metadata = _fields(body['receipt'], {'lake_id', 'day', 'revision'})
        receipt = OfficialReceipt(metadata['lake_id'], metadata['day'], metadata['revision'])
        coverage = []
        for raw in _array(body['coverage']):
            _fields(raw, {'partition', 'revision'})
            coverage.append(PartitionCoverage(raw['partition'], raw['revision']))
        manifest = ReleaseManifest(receipt, tuple(coverage))
        rows = tuple(_decode_row(raw) for raw in _array(body['rows']))
        return PartitionPackage(body['package_id'], body['release_id'], body['partition'],
                                manifest, rows)
    except PackageFormatError:
        raise
    except (ValueError, TypeError, RecursionError) as exc:
        raise PackageFormatError('invalid package encoding') from exc
