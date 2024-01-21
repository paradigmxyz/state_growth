from __future__ import annotations

import polars as pl


def aggregate_balance_diffs(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by('block_number')
        .agg(
            n_balance_diffs=pl.len(),
            n_written_balances=pl.col.address.n_unique(),
            n_new_accounts=pl.col.address.filter(pl.col.from_value_string == '0').len(),
        )
        .sort('block_number')
    )
