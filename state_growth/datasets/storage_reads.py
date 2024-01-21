from __future__ import annotations

import polars as pl

from state_growth import FrameType


def aggregate_storage_reads(
    df: FrameType, *, group_by: str = 'block_number'
) -> FrameType:
    return (
        df.group_by(group_by)
        .agg(
            n_storage_reads=pl.len(),
            n_read_storage_contracts=pl.col.contract_address.n_unique(),
            n_read_storage_slots=pl.struct(['contract_address', 'slot']).n_unique(),
        )
        .sort(group_by)
    )
