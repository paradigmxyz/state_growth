from __future__ import annotations

import polars as pl

from ..spec import binary_zero_word, FrameType


def aggregate_storage_diffs(
    df: FrameType, *, group_by: str = 'block_number'
) -> FrameType:
    return (
        df.group_by(group_by)
        .agg(
            n_storage_diffs=pl.len(),
            n_written_storage_contracts=pl.col.address.n_unique(),
            n_written_storage_slots=pl.struct(['address', 'slot']).n_unique(),
            n_new_storage_slots=(
                pl.struct(['address', 'slot'])
                .filter(pl.col.from_value == binary_zero_word)
                .n_unique()
            ),
        )
        .sort(group_by)
    )
