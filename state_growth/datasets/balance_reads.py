from __future__ import annotations

import polars as pl

from ..spec import FrameType
from ..schema_utils import get_schema_agg, AggSchema

schema: AggSchema = {
    'columns': {
        'n_read_balance_addresses': {
            'type': 'unique',
            'agg': pl.col.address.n_unique(),
        },
        'n_balance_reads': {'type': 'count', 'agg': pl.len()},
    },
}


def aggregate_balance_reads(
    df: FrameType, *, group_by: str = 'block_number'
) -> FrameType:
    return df.group_by(group_by).agg(**get_schema_agg(schema)).sort(group_by)
