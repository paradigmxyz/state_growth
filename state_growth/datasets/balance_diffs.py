from __future__ import annotations

import polars as pl

from state_growth import FrameType


def aggregate_balance_diffs(
    df: FrameType, *, group_by: str = 'block_number'
) -> FrameType:
    return (
        df.group_by(group_by)
        .agg(
            n_balance_diffs=pl.len(),
            n_written_balances=pl.col.address.n_unique(),
            n_new_accounts=pl.col.address.filter(pl.col.from_value_string == '0').len(),
        )
        .sort(group_by)
    )
