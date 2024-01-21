from __future__ import annotations

import polars as pl


def aggregate_storage_reads(
    df: pl.DataFrame, *, group_by: str = 'block_number'
) -> pl.DataFrame:
    return (
        df.group_by(group_by)
        .agg(
            n_storage_reads=pl.len(),
            n_read_storage_contracts=pl.col.contract_address.n_unique(),
            n_read_storage_slots=pl.struct(['contract_address', 'slot']).n_unique(),
        )
        .sort(group_by)
    )
