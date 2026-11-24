from lake.query.reconcile import reconcile_account
from lake.query.rows import assemble_rows
from lake.query.scope import InvalidMirrorRequest, normalize_request, validate_official_receipt
from lake.query.visibility import PendingMaterials, RetiredMaterials, select_visible_materials
from lake.receiving.inventory import read_received_evidence
from lake.receiving.store import mirror_read_transaction


def mirror_read(db, receipt, partitions):
    request = normalize_request(receipt, partitions)
    with mirror_read_transaction(db):
        evidence = read_received_evidence(db, request.receipt)
        selection = select_visible_materials(request, evidence)
        if isinstance(selection, PendingMaterials):
            return selection
        rows = assemble_rows(selection)
        return reconcile_account(request, selection.manifest.receipt, rows)


def read_rollover(mirror, medium, receipt):
    from lake.receiving.rollover import RolloverRejected, read_rollover_account
    try:
        validate_official_receipt(receipt)
    except ValueError as exc:
        raise InvalidMirrorRequest(str(exc)) from exc
    try:
        account = read_rollover_account(mirror, medium, receipt)
    except RolloverRejected as exc:
        raise InvalidMirrorRequest(str(exc)) from exc
    if isinstance(account, PendingMaterials):
        return PendingMaterials(receipt, account.partitions)
    if isinstance(account, RetiredMaterials):
        return RetiredMaterials(receipt)
    return account
