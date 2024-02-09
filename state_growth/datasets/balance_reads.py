from __future__ import annotations

import polars as pl

import state_growth

schema: state_growth.AggSchema = {
    'columns': {
        'n_read_balance_addresses': {
            'type': 'unique',
            'agg': pl.col.address.n_unique(),
        },
        'n_balance_reads': {'type': 'count', 'agg': pl.len()},
    },
}


def aggregate_balance_reads(
    df: state_growth.FrameType, *, group_by: str = 'block_number'
) -> state_growth.FrameType:
    return (
        df.group_by(group_by)
        .agg(**state_growth.get_schema_agg(schema))
        .sort(group_by)
    )

