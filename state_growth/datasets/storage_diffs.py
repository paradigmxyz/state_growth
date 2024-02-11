from __future__ import annotations

import polars as pl

import state_growth


schema: state_growth.AggSchema = {
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
                .filter(pl.col.to_value == state_growth.binary_zero_word)
                .len()
            ),
        },
        'n_new_storage_slots': {
            'type': 'unique',
            'agg': (
                pl.struct(['address', 'slot'])
                .filter(pl.col.from_value == state_growth.binary_zero_word)
                .len()
            ),
        },
    },
}


def aggregate_storage_diffs(
    df: state_growth.FrameType, *, group_by: str = 'block_number'
) -> state_growth.FrameType:
    return (
        df.group_by(group_by).agg(**state_growth.get_schema_agg(schema)).sort(group_by)
    )
