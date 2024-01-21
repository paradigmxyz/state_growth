from __future__ import annotations

import polars as pl

from state_growth import FrameType


def aggregate_transactions(
    df: FrameType, *, group_by: str = 'block_number'
) -> FrameType:
    return (
        df.group_by(group_by)
        .agg(
            n_txs=pl.len(),
            n_from_addresses=pl.col.from_address.n_unique(),
            n_to_addresses=pl.col.to_address.n_unique(),
            gas_used=pl.sum('gas_used'),
            n_input_bytes=pl.sum('n_input_bytes'),
            n_input_zero_bytes=pl.sum('n_input_zero_bytes'),
            n_input_nonzero_bytes=pl.sum('n_input_nonzero_bytes'),
            n_rlp_bytes=pl.sum('n_rlp_bytes'),
        )
        .sort(group_by)
    )
