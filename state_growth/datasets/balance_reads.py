from __future__ import annotations

import polars as pl


def aggregate_balance_diffs(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by('block_number')
        .agg(
            n_read_balance_addresses=pl.col.address.n_unique(),
            n_balance_reads=pl.len(),
        )
        .sort('block_number')
    )
