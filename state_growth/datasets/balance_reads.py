from __future__ import annotations

import polars as pl

from state_growth import FrameType


def aggregate_balance_reads(
    df: FrameType, *, group_by: str = 'block_number'
) -> FrameType:
    return (
        df.group_by(group_by)
        .agg(
            n_read_balance_addresses=pl.col.address.n_unique(),
            n_balance_reads=pl.len(),
        )
        .sort(group_by)
    )
