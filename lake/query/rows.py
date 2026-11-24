from lake.dispatch.partition_package import PackageRow
from lake.query.visibility import VisibleSelection


def assemble_rows(selection: VisibleSelection) -> tuple[PackageRow, ...]:
    return tuple(sorted((row for package in selection.packages for row in package.rows),
                        key=lambda row: row.position))
