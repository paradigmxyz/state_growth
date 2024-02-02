from __future__ import annotations

import polars as pl

from ..spec import binary_zero_word, FrameType
from ..schema_utils import get_schema_agg, AggSchema


schema: AggSchema = {
    'columns': {
        'n_storage_diffs': {'type': 'count', 'agg': pl.len()},
        'n_written_storage_contracts': {
            'type': 'unique',
            'agg': pl.col.address.n_unique(),
        },
        'n_written_storage_slots': {
            'type': 'unique',
            'agg': pl.struct(['address', 'slot']).n_unique(),
        },
        'n_deleted_storage_slots': {
            'type': 'unique',
            'agg': (
                pl.struct(['address', 'slot'])
                .filter(pl.col.to_value == binary_zero_word)
                .len()
            ),
        },
        'n_new_storage_slots': {
            'type': 'unique',
            'agg': (
                pl.struct(['address', 'slot'])
                .filter(pl.col.from_value == binary_zero_word)
                .len()
            ),
        },
    },
}


def aggregate_storage_diffs(
    df: FrameType, *, group_by: str = 'block_number'
) -> FrameType:
    return df.group_by(group_by).agg(**get_schema_agg(schema)).sort(group_by)
