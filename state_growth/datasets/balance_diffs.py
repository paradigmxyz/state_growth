from __future__ import annotations

import polars as pl

import state_growth


schema: state_growth.AggSchema = {
    'columns': {
        'n_balance_diffs': {'type': 'count', 'agg': pl.len()},
        'n_written_balances': {
            'type': 'unique',
            'agg': pl.col.address.n_unique(),
        },
        'n_new_accounts': {
            'type': 'count',
            'agg': pl.col.address.filter(pl.col.from_value_string == '0').len(),
        },
    },
}


def aggregate_balance_diffs(
    df: state_growth.FrameType, *, group_by: str = 'block_number'
) -> state_growth.FrameType:
    return (
        df.group_by(group_by).agg(**state_growth.get_schema_agg(schema)).sort(group_by)
    )
