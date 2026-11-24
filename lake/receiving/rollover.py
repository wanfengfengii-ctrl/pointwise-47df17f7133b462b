"""Cold-mirror rollover: directory schema, state and operations.

The directory (the mirror database) commits separately from the fixed-slot
medium and can never roll back a medium write.  Row bodies live only in
medium slots; the directory keeps identities, digests and bookkeeping.

Every mutating step is ordered so that an interrupted call can be resumed
with the same arguments after closing and reopening both files:

* accepting backs the old target body up into the spare slot before the main
  slot is overwritten, and only then commits the acceptance;
* retiring verifies that the new version is served by main slots alone, then
  marks the old receipt retired in the directory *before* clearing the spare
  slot, and only reports success once the spare slot is actually empty.
"""
from dataclasses import dataclass
import hashlib

from lake.database import atomic
from lake.dispatch.rollover_wire import (
    RolloverFormatError, decode_rollover, encode_rollover, row_digest, rows_digest,
)
from lake.intake.source_rows import ObservationKey
from lake.intake.validate import validate_name, validate_positive_id
from lake.ledger.revisions import OfficialReceipt, validate_official_receipt
from lake.receiving.receipt import ReceivingReceipt
from lake.receiving.slots import FixedSlots, SlotRecord

SEEDED = 'seeded'
ACCEPTED = 'accepted'


class RolloverRejected(ValueError):
    pass


@dataclass(frozen=True)
class RolloverBook:
    lake_id: str
    day: str
    partition: str
    old_receipt: OfficialReceipt
    new_revision: int | None
    old_rows_digest: str
    status: str
    retired: bool
    receiving_id: int | None


@dataclass(frozen=True)
class RolloverRowSlot:
    address: int
    key: ObservationKey
    position: int
    old_digest: str
    new_digest: str | None
    target: bool


# ---------------------------------------------------------------------------
# directory schema and state


def create_rollover_directory(db):
    db.execute('''CREATE TABLE IF NOT EXISTS rollover_book (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        lake_id TEXT NOT NULL,
        day TEXT NOT NULL,
        partition TEXT NOT NULL,
        old_revision INTEGER NOT NULL,
        new_revision INTEGER,
        old_rows_digest TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('seeded', 'accepted')),
        retired INTEGER NOT NULL DEFAULT 0 CHECK (retired IN (0, 1)),
        receiving_id INTEGER
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS rollover_rows (
        address INTEGER PRIMARY KEY,
        batch_id TEXT NOT NULL,
        observation TEXT NOT NULL,
        position INTEGER NOT NULL,
        old_digest TEXT NOT NULL,
        new_digest TEXT,
        target INTEGER NOT NULL DEFAULT 0 CHECK (target IN (0, 1))
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS rollover_acceptance (
        receiving_id INTEGER PRIMARY KEY AUTOINCREMENT,
        package_id TEXT NOT NULL UNIQUE,
        payload_digest TEXT NOT NULL,
        new_revision INTEGER NOT NULL
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS rollover_operations (
        operation_id TEXT PRIMARY KEY,
        old_revision INTEGER NOT NULL,
        new_revision INTEGER NOT NULL,
        completed INTEGER NOT NULL DEFAULT 0 CHECK (completed IN (0, 1))
    )''')


def _has_directory(db):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' "
                      "AND name = 'rollover_book'").fetchone() is not None


def read_book(db):
    if not _has_directory(db):
        return None
    record = db.execute('''SELECT lake_id, day, partition, old_revision, new_revision,
        old_rows_digest, status, retired, receiving_id
        FROM rollover_book WHERE singleton = 1''').fetchone()
    if record is None:
        return None
    (lake_id, day, partition, old_revision, new_revision, old_digest, status,
     retired, receiving_id) = record
    return RolloverBook(lake_id, day, partition,
                        OfficialReceipt(lake_id, day, old_revision),
                        new_revision, old_digest, status, bool(retired), receiving_id)


def read_rows(db):
    return tuple(
        RolloverRowSlot(address, ObservationKey(batch_id, observation), position,
                        old_digest, new_digest, bool(target))
        for address, batch_id, observation, position, old_digest, new_digest, target
        in db.execute('''SELECT address, batch_id, observation, position,
            old_digest, new_digest, target FROM rollover_rows ORDER BY address''')
    )


def write_seed(db, partition, receipt, rows):
    db.execute('''INSERT INTO rollover_book
        (singleton, lake_id, day, partition, old_revision, new_revision,
         old_rows_digest, status) VALUES (1, ?, ?, ?, ?, NULL, ?, 'seeded')''',
               (receipt.lake_id, receipt.day, partition, receipt.revision, rows_digest(rows)))
    db.executemany('''INSERT INTO rollover_rows
        (address, batch_id, observation, position, old_digest, target)
        VALUES (?, ?, ?, ?, ?, 0)''', (
        (address, row.key.batch_id, row.key.observation, row.position, row_digest(row))
        for address, row in enumerate(rows)
    ))


# ---------------------------------------------------------------------------
# validation helpers


def _reject(message):
    return RolloverRejected(message)


def _name(value, label):
    try:
        return validate_name(value, label)
    except ValueError as exc:
        raise _reject(str(exc)) from exc


def _receipt(receipt):
    try:
        return validate_official_receipt(receipt)
    except ValueError as exc:
        raise _reject(str(exc)) from exc


def _positive(value, label):
    try:
        return validate_positive_id(value, label)
    except ValueError as exc:
        raise _reject(str(exc)) from exc


def _check_medium(medium):
    if not isinstance(medium, FixedSlots):
        raise _reject('expected a fixed-slot medium')


def _validate_package(package):
    """Structural checks that need no local account."""
    _receipt(package.old_receipt)
    _receipt(package.new_receipt)
    if (package.old_receipt.lake_id, package.old_receipt.day) != \
            (package.new_receipt.lake_id, package.new_receipt.day):
        raise _reject('rollover receipts belong to different observation books')
    _name(package.partition, 'rollover partition')
    _name(package.package_id, 'rollover package id')
    if package.new_receipt.revision != package.old_receipt.revision + 1:
        raise _reject('rollover receipts are not direct parent and child')
    for value, label in ((package.old_digest, 'old projection digest'),
                         (package.new_digest, 'new projection digest'),
                         (package.target.old_digest, 'old target digest'),
                         (package.target.before_digest, 'before digest'),
                         (package.target.after_digest, 'after digest')):
        if value is not None and (type(value) is not str or len(value) != 64
                                  or any(char not in '0123456789abcdef' for char in value)):
            raise _reject(f'invalid {label}')
    if not package.old_order:
        raise _reject('rollover package carries no original order')
    seen = set()
    previous_position = 0
    for entry in package.old_order:
        _name(entry.batch_id, 'order batch')
        _name(entry.observation, 'order observation')
        _positive(entry.position, 'order position')
        if entry.position <= previous_position:
            raise _reject('original order is not strictly increasing')
        previous_position = entry.position
        if entry.key in seen:
            raise _reject('duplicate observation in original order')
        seen.add(entry.key)
    target = package.target
    _name(target.batch_id, 'target batch')
    _name(target.observation, 'target observation')
    _positive(target.position, 'target position')
    matching = [entry for entry in package.old_order if entry.key == target.key]
    if len(matching) != 1 or matching[0].position != target.position:
        raise _reject('target row is not pinned to the original order')
    row = package.row
    if type(row.key) is not ObservationKey:
        raise _reject('expected an observation key in the replacement row')
    if (row.key, row.partition, row.position) != (target.key, package.partition, target.position):
        raise _reject('replacement row does not occupy the target slot')
    if type(row.source) is not str or not row.source.strip() or not row.source.isprintable():
        raise _reject('invalid replacement source')
    _positive(row.line, 'replacement source line')
    if row.mm is not None and type(row.mm) is not int:
        raise _reject('invalid replacement millimetres')


def _decode(payload):
    try:
        package = decode_rollover(payload)
    except RolloverFormatError as exc:
        raise _reject(str(exc)) from exc
    except ValueError as exc:
        raise _reject('invalid rollover package') from exc
    _validate_package(package)
    return package


def _seed_package(payload):
    from lake.receiving.package_checks import check_package
    try:
        return check_package(payload)
    except ValueError as exc:
        raise _reject(str(exc)) from exc


def _sealed(package):
    sealed = encode_rollover(package)
    return sealed, hashlib.sha256(sealed).hexdigest()


# ---------------------------------------------------------------------------
# medium helpers


def _entry_by_target(directory_rows, package):
    matches = [entry for entry in directory_rows
               if entry.key == package.target.key and entry.position == package.target.position]
    if len(matches) != 1:
        raise _reject('target row is not in the cold mirror account')
    return matches[0]


def _read_old_projection(medium, directory_rows, target, package):
    """Assemble the old projection, tolerating a target swap already applied."""
    rows = []
    spare = medium.read(medium.spare)
    new_digest = row_digest(package.row)
    for entry in directory_rows:
        record = medium.read(entry.address)
        if record is None or (record.row.key, record.row.position) != (entry.key, entry.position):
            raise _reject('a main slot no longer holds the old projection')
        current = row_digest(record.row)
        if entry.address == target.address and current == new_digest:
            if spare is None:
                raise _reject('the old target row is no longer available')
            if (spare.row.key, spare.row.position) != (entry.key, entry.position) \
                    or row_digest(spare.row) != entry.old_digest:
                raise _reject('the spare slot does not hold the old target row')
            rows.append(spare.row)
            continue
        if current != entry.old_digest:
            raise _reject('a main slot no longer holds the old projection')
        rows.append(record.row)
    return tuple(rows)


def _bind_package(medium, book, directory_rows, package):
    if book is None:
        raise _reject('cold mirror is not initialized')
    if (book.lake_id, book.day) != (package.old_receipt.lake_id, package.old_receipt.day):
        raise _reject('rollover package belongs to another observation book')
    if book.partition != package.partition:
        raise _reject('rollover package names another partition')
    if book.old_receipt != package.old_receipt:
        raise _reject('rollover package names another old receipt')
    if len(directory_rows) != len(package.old_order):
        raise _reject('rollover package does not match the old projection')
    for entry, order in zip(directory_rows, package.old_order):
        if (entry.key, entry.position) != (order.key, order.position):
            raise _reject('rollover package reorders the old projection')
    if book.old_rows_digest != package.old_digest:
        raise _reject('rollover package names another old projection')
    target = _entry_by_target(directory_rows, package)
    if target.old_digest != package.target.old_digest:
        raise _reject('rollover package names another target row')
    index = directory_rows.index(target)
    before = directory_rows[index - 1].old_digest if index > 0 else None
    after = directory_rows[index + 1].old_digest if index + 1 < len(directory_rows) else None
    if (package.target.before_digest, package.target.after_digest) != (before, after):
        raise _reject('rollover package names other neighbouring rows')

    old_rows = _read_old_projection(medium, directory_rows, target, package)
    if rows_digest(old_rows) != package.old_digest:
        raise _reject('cold mirror holds another old projection')
    new_rows = tuple(package.row if entry.address == target.address else row
                     for entry, row in zip(directory_rows, old_rows))
    if rows_digest(new_rows) != package.new_digest:
        raise _reject('rollover package names another new projection')
    return target, old_rows


# ---------------------------------------------------------------------------
# public operations


def initialize_rollover(mirror, medium, old_payload):
    _check_medium(medium)
    package = _seed_package(old_payload)
    receipt = _receipt(package.manifest.receipt)
    _name(package.partition, 'rollover partition')
    if not package.rows:
        raise _reject('a cold rollover needs a nonempty partition')
    if len(package.rows) != medium.main_count:
        raise _reject('medium slot count does not match the old partition package')
    positions = [row.position for row in package.rows]
    if positions != sorted(set(positions)):
        raise _reject('old partition package is not in official original order')

    with atomic(mirror, write=False):
        book = read_book(mirror)
        directory_rows = read_rows(mirror) if book is not None else ()
    if book is not None:
        if (book.lake_id, book.day, book.partition) != \
                (receipt.lake_id, receipt.day, package.partition) \
                or book.old_receipt != receipt \
                or book.old_rows_digest != rows_digest(package.rows):
            raise _reject('cold mirror is bound to another rollover')
        if len(directory_rows) != len(package.rows):
            raise _reject('cold mirror directory does not match the old projection')
        for entry, row in zip(directory_rows, package.rows):
            if (entry.key, entry.position, entry.old_digest) != \
                    (row.key, row.position, row_digest(row)):
                raise _reject('cold mirror directory does not match the old projection')
            stored = medium.read(entry.address)
            if stored is None or stored.row != row:
                raise _reject('medium no longer matches the seeded old projection')
        if medium.read(medium.spare) is not None:
            raise _reject('the spare slot is not empty')
        return receipt

    for address, row in enumerate(package.rows):
        record = medium.read(address)
        if record is not None and record.row != row:
            raise _reject('a cold rollover must be built on an empty medium')
    if medium.read(medium.spare) is not None:
        raise _reject('a cold rollover must be built on an empty medium')
    for address, row in enumerate(package.rows):
        if medium.read(address) is None:
            medium.replace(address, SlotRecord(row, f'rollover/old/{receipt.revision}/{address}'))
    with atomic(mirror):
        create_rollover_directory(mirror)
        if read_book(mirror) is not None:
            raise _reject('cold mirror was initialized concurrently')
        write_seed(mirror, package.partition, receipt, package.rows)
    return receipt


def _first_receipt(mirror, package):
    record = mirror.execute('''SELECT receiving_id, payload_digest, new_revision
        FROM rollover_acceptance WHERE package_id = ?''', (package.package_id,)).fetchone()
    if record is None:
        return None
    receiving_id, payload_digest, new_revision = record
    if new_revision != package.new_receipt.revision:
        raise _reject('another rollover package already has this identity')
    return ReceivingReceipt(receiving_id, package.package_id), payload_digest


def receive_rollover(mirror, medium, payload):
    _check_medium(medium)
    package = _decode(payload)
    _, digest = _sealed(package)

    with atomic(mirror, write=False):
        book = read_book(mirror)
        directory_rows = read_rows(mirror) if book is not None else ()
        first = _first_receipt(mirror, package) if book is not None else None
    if first is not None:
        receipt, stored_digest = first
        if stored_digest != digest:
            raise _reject('a different rollover package already has this identity')
        return receipt
    if book is None:
        raise _reject('cold mirror is not initialized')
    if book.status == ACCEPTED:
        raise _reject('cold mirror already accepted another rollover package')
    if book.new_revision is not None and book.new_revision != package.new_receipt.revision:
        raise _reject('cold mirror already holds another new receipt')

    target, old_rows = _bind_package(medium, book, directory_rows, package)
    new_digest = row_digest(package.row)
    target_body = next(row for entry, row in zip(directory_rows, old_rows)
                       if entry.address == target.address)

    spare = medium.read(medium.spare)
    if spare is None:
        medium.replace(medium.spare, SlotRecord(
            target_body, f'rollover/old-target/{package.package_id}'))
    elif (spare.row.key, spare.row.position) != (target.key, target.position) \
            or row_digest(spare.row) != target.old_digest:
        raise _reject('the spare slot holds an unexpected row')
    current = medium.read(target.address)
    if current is None or row_digest(current.row) != new_digest:
        medium.replace(target.address, SlotRecord(
            package.row, f'rollover/new/{package.new_receipt.revision}/{target.address}'))

    with atomic(mirror):
        stored = read_book(mirror)
        if stored is None or stored.status != SEEDED:
            raise _reject('cold mirror changed while receiving')
        insert = mirror.execute('''INSERT INTO rollover_acceptance
            (package_id, payload_digest, new_revision) VALUES (?, ?, ?)''',
                                (package.package_id, digest, package.new_receipt.revision))
        mirror.execute('''UPDATE rollover_book SET status = ?, new_revision = ?,
            receiving_id = ? WHERE singleton = 1''',
                       (ACCEPTED, package.new_receipt.revision, insert.lastrowid))
        mirror.execute('''UPDATE rollover_rows SET target = 1, new_digest = ?
            WHERE address = ?''', (new_digest, target.address))
        return ReceivingReceipt(insert.lastrowid, package.package_id)


def _new_main_rows(medium, directory_rows):
    rows = []
    target = None
    for entry in directory_rows:
        record = medium.read(entry.address)
        if record is None or (record.row.key, record.row.position) != (entry.key, entry.position):
            raise _reject('the new version is not served by the main slots')
        current = row_digest(record.row)
        if entry.target:
            target = entry
            if entry.new_digest is None or current != entry.new_digest:
                raise _reject('the new target row is not in its main slot')
        elif current != entry.old_digest:
            raise _reject('the new version is not served by the main slots')
        rows.append(record.row)
    if target is None:
        raise _reject('cold mirror has no target row')
    return target, tuple(rows)


def _operation(mirror, operation_id):
    return mirror.execute('''SELECT old_revision, new_revision, completed
        FROM rollover_operations WHERE operation_id = ?''', (operation_id,)).fetchone()


def retire_rollover(mirror, medium, operation_id, old):
    _check_medium(medium)
    _name(operation_id, 'operation id')
    _receipt(old)

    with atomic(mirror, write=False):
        book = read_book(mirror)
        directory_rows = read_rows(mirror) if book is not None else ()
        operation = _operation(mirror, operation_id)
        any_operation = mirror.execute('SELECT operation_id FROM rollover_operations '
                                       'WHERE operation_id <> ?', (operation_id,)).fetchone()
    if book is None:
        raise _reject('cold mirror is not initialized')
    if book.old_receipt != old:
        raise _reject('retirement names another old receipt')
    if book.new_revision is None or book.status != ACCEPTED:
        raise _reject('the new version has not been received')
    new = OfficialReceipt(book.lake_id, book.day, book.new_revision)
    if operation is not None:
        recorded_old, recorded_new, completed = operation
        if (recorded_old, recorded_new) != (old.revision, new.revision):
            raise _reject('operation id is bound to other arguments')
        if completed:
            # A repeated success call still requires the new version to come
            # from main slots alone and the spare slot to be actually empty.
            _new_main_rows(medium, directory_rows)
            if medium.read(medium.spare) is not None:
                raise _reject('the spare slot is not empty')
            return operation_id, old, new
    elif any_operation is not None:
        raise _reject('another retirement is already known')
    elif book.retired:
        raise _reject('the old receipt is already retired')

    target, _ = _new_main_rows(medium, directory_rows)
    if operation is None:
        with atomic(mirror):
            stored = read_book(mirror)
            if stored is None or stored.status != ACCEPTED or stored.retired:
                raise _reject('cold mirror changed while retiring')
            mirror.execute('''INSERT INTO rollover_operations
                (operation_id, old_revision, new_revision) VALUES (?, ?, ?)''',
                           (operation_id, old.revision, new.revision))
            mirror.execute('UPDATE rollover_book SET retired = 1 WHERE singleton = 1')
    elif not book.retired:
        raise _reject('cold mirror lost its retirement mark')

    spare = medium.read(medium.spare)
    if spare is not None:
        if (spare.row.key, spare.row.position) != (target.key, target.position) \
                or row_digest(spare.row) != target.old_digest:
            raise _reject('the spare slot holds an unexpected row')
        medium.clear_spare()
    if medium.read(medium.spare) is not None:
        raise _reject('the spare slot is not empty')
    with atomic(mirror):
        mirror.execute('UPDATE rollover_operations SET completed = 1 WHERE operation_id = ?',
                       (operation_id,))
    return operation_id, old, new


# ---------------------------------------------------------------------------
# reads


def _old_projection(medium, book, directory_rows):
    """Assemble the old projection, tolerating a target swap already applied.

    An interrupted acceptance may already have overwritten a main slot while
    the old body is preserved in the spare slot; that body still belongs to
    the old version and is used here.
    """
    rows = []
    for entry in directory_rows:
        record = medium.read(entry.address)
        if record is None or (record.row.key, record.row.position) != (entry.key, entry.position):
            raise _reject('the old version is no longer available')
        current = row_digest(record.row)
        if current == entry.old_digest:
            rows.append(record.row)
            continue
        spare = medium.read(medium.spare)
        if spare is None or (spare.row.key, spare.row.position) != \
                (entry.key, entry.position) or row_digest(spare.row) != entry.old_digest:
            raise _reject('the old version is no longer available')
        rows.append(spare.row)
    if rows_digest(rows) != book.old_rows_digest:
        raise _reject('the old version no longer matches the cold mirror')
    return tuple(rows)


def _new_projection(medium, book, directory_rows):
    rows = []
    target = None
    for entry in directory_rows:
        record = medium.read(entry.address)
        if record is None or (record.row.key, record.row.position) != (entry.key, entry.position):
            raise _reject('the new version is not available')
        current = row_digest(record.row)
        if entry.target:
            target = entry
            if entry.new_digest is None or current != entry.new_digest:
                raise _reject('the new version is not available')
        elif current != entry.old_digest:
            raise _reject('the new version is not available')
        rows.append(record.row)
    if target is None:
        raise _reject('cold mirror has no target row')
    return tuple(rows)


def read_rollover_account(mirror, medium, receipt):
    """Return the cold-mirror account for a receipt using local material only."""
    _check_medium(medium)
    _receipt(receipt)
    from lake.query.reconcile import reconcile_account
    from lake.query.scope import MirrorRequest
    from lake.query.visibility import PendingMaterials, RetiredMaterials

    with atomic(mirror, write=False):
        book = read_book(mirror)
        directory_rows = read_rows(mirror) if book is not None else ()
    request = MirrorRequest(receipt, (book.partition,)) if book is not None else None
    if book is not None and receipt == book.old_receipt:
        if book.retired:
            return RetiredMaterials(receipt)
        return reconcile_account(request, receipt, _old_projection(medium, book, directory_rows))
    if book is not None and book.new_revision is not None and book.status == ACCEPTED \
            and receipt == OfficialReceipt(book.lake_id, book.day, book.new_revision):
        return reconcile_account(request, receipt, _new_projection(medium, book, directory_rows))
    # A well-formed receipt with no local account of its own is still pending.
    return PendingMaterials(receipt, () if book is None else (book.partition,))
