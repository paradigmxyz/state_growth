from __future__ import annotations

import polars as pl

import state_growth


schema: state_growth.AggSchema = {
    'columns': {
        'n_contract_deploys': {'type': 'count', 'agg': pl.len()},
        'n_unique_factories': {
            'type': 'count',
            'agg': pl.col.factory.n_unique(),
        },
        'n_unique_deployers': {
            'type': 'count',
            'agg': pl.col.deployer.n_unique(),
        },
        'n_code_bytes_deployed': {
            'type': 'count',
            'agg': pl.col.n_code_bytes.cast(pl.UInt64).sum(),
        },
        'n_init_code_bytes': {
            'type': 'count',
            'agg': pl.col.n_init_code_bytes.cast(pl.UInt64).sum(),
        },
    },
}


def aggregate_contracts(
    df: state_growth.FrameType, *, group_by: str = 'block_number'
) -> state_growth.FrameType:
    return (
        df.group_by(group_by).agg(**state_growth.get_schema_agg(schema)).sort(group_by)
    )
