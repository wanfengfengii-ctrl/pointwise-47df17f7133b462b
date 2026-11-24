from dataclasses import dataclass


@dataclass(frozen=True)
class PartitionTotals:
    partition: str
    total_mm: int
    missing_count: int
    row_count: int


@dataclass(frozen=True)
class Totals:
    total_mm: int
    missing_count: int
    row_count: int
    by_partition: tuple[PartitionTotals, ...]


def calculate_totals(rows, partitions):
    sums = dict.fromkeys(partitions, 0)
    missing = dict.fromkeys(sums, 0)
    counts = dict.fromkeys(sums, 0)
    for row in rows:
        counts[row.partition] += 1
        if row.mm is None:
            missing[row.partition] += 1
        else:
            sums[row.partition] += row.mm
    subtotals = tuple(PartitionTotals(name, sums[name], missing[name], counts[name])
                      for name in sorted(sums))
    return Totals(sum(sums.values()), sum(missing.values()), sum(counts.values()), subtotals)
