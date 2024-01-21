from __future__ import annotations

import polars as pl

from ..spec import FrameType
from ..schema_utils import get_schema_agg, AggSchema


schema: AggSchema = {
    'columns': {
        'n_txs': {'type': 'count', 'agg': pl.len()},
        'n_from_addresses': {'type': 'unique', 'agg': pl.col.from_address.n_unique()},
        'n_to_addresses': {'type': 'unique', 'agg': pl.col.to_address.n_unique()},
        'gas_used': {'type': 'count', 'agg': pl.sum('gas_used')},
        'n_input_bytes': {'type': 'count', 'agg': pl.sum('n_input_bytes')},
        'n_input_zero_bytes': {'type': 'count', 'agg': pl.sum('n_input_zero_bytes')},
        'n_input_nonzero_bytes': {
            'type': 'count',
            'agg': pl.sum('n_input_nonzero_bytes'),
        },
        'n_rlp_bytes': {'type': 'count', 'agg': pl.sum('n_rlp_bytes')},
    },
}


def aggregate_transactions(
    df: FrameType, *, group_by: str = 'block_number'
) -> FrameType:
    return df.group_by(group_by).agg(**get_schema_agg(schema)).sort(group_by)
