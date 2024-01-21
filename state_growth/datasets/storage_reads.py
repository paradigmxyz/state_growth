from __future__ import annotations

import polars as pl

from ..spec import FrameType
from ..schema_utils import get_schema_agg, AggSchema

schema: AggSchema = {
    'columns': {
        'n_storage_reads': {'type': 'count', 'agg': pl.len()},
        'n_read_storage_contracts': {
            'type': 'unique',
            'agg': pl.col.contract_address.n_unique(),
        },
        'n_read_storage_slots': {
            'type': 'unique',
            'agg': pl.struct(['contract_address', 'slot']).n_unique(),
        },
    },
}


def aggregate_storage_reads(
    df: FrameType, *, group_by: str = 'block_number'
) -> FrameType:
    return df.group_by(group_by).agg(**get_schema_agg(schema)).sort(group_by)
