from __future__ import annotations

import polars as pl

from ..spec import FrameType
from ..schema_utils import get_schema_agg, AggSchema


schema: AggSchema = {
    'columns': {
        'n_balance_diffs': {'type': 'count', 'agg': pl.len()},
        'n_written_balances': {'type': 'unique', 'agg': pl.col.address.n_unique()},
        'n_new_accounts': {
            'type': 'count',
            'agg': pl.col.address.filter(pl.col.from_value_string == '0').len(),
        },
    },
}


def aggregate_balance_diffs(
    df: FrameType, *, group_by: str = 'block_number'
) -> FrameType:
    return df.group_by(group_by).agg(**get_schema_agg(schema)).sort(group_by)
