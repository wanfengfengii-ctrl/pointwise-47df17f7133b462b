from lake.query.reconcile import reconcile_account
from lake.query.rows import assemble_rows
from lake.query.scope import normalize_request
from lake.query.visibility import PendingMaterials, select_visible_materials
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
