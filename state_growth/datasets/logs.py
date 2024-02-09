from __future__ import annotations

import polars as pl

import state_growth


WETH = bytes.fromhex(state_growth.erc20s['weth'][2:])
USDT = bytes.fromhex(state_growth.erc20s['usdt'][2:])
USDC = bytes.fromhex(state_growth.erc20s['usdc'][2:])

schema: state_growth.AggSchema = {
    'columns': {
        'n_logs': {'type': 'count', 'agg': pl.len()},
        'n_topics': {
            'type': 'count',
            'agg': (
                4 * pl.len()
                - pl.col('topic0').null_count()
                - pl.col('topic1').null_count()
                - pl.col('topic2').null_count()
                - pl.col('topic3').null_count()
            ),
        },
        'n_event_types': {'type': 'unique', 'agg': pl.col.topic0.n_unique()},
        'n_log_data_bytes': {'type': 'count', 'agg': pl.sum('n_data_bytes')},
    },
}

erc20_transfers_schema: state_growth.AggSchema = {
    'columns': {
        'erc20_transfers': {'type': 'count', 'agg': pl.len()},
        'weth_transfers': {
            'type': 'count',
            'agg': pl.col.address.filter(pl.col.address == WETH).count(),
        },
        'usdt_transfers': {
            'type': 'count',
            'agg': pl.col.address.filter(pl.col.address == USDT).count(),
        },
        'usdc_transfers': {
            'type': 'count',
            'agg': pl.col.address.filter(pl.col.address == USDC).count(),
        },
    },
}

erc20_approvals_schema: state_growth.AggSchema = {
    'columns': {
        'erc20_approvals': {'type': 'count', 'agg': pl.len()},
        'weth_approvals': {
            'type': 'count',
            'agg': pl.col.address.filter(pl.col.address == WETH).count(),
        },
        'usdt_approvals': {
            'type': 'count',
            'agg': pl.col.address.filter(pl.col.address == USDT).count(),
        },
        'usdc_approvals': {
            'type': 'count',
            'agg': pl.col.address.filter(pl.col.address == USDC).count(),
        },
    },
}

erc721_transfers_schema: state_growth.AggSchema = {
    'columns': {
        'erc721_transfers': {'type': 'count', 'agg': pl.len()},
    },
}

erc721_approvals_schema: state_growth.AggSchema = {
    'columns': {
        'erc721_approvals': {'type': 'count', 'agg': pl.len()},
    },
}


def aggregate_logs(
    df: state_growth.FrameType, *, group_by: str = 'block_number'
) -> state_growth.FrameType:
    transfer = bytes.fromhex(state_growth.event_types['transfer'][2:])
    approval = bytes.fromhex(state_growth.event_types['approval'][2:])

    erc20_transfers_per_block = (
        df.filter((pl.col.topic0 == transfer) & (pl.col.topic3.is_null()))
        .group_by(group_by)
        .agg(**state_growth.get_schema_agg(erc20_transfers_schema))
    )

    erc20_approvals_per_block = (
        df.filter((pl.col.topic0 == approval) & (pl.col.topic3.is_null()))
        .group_by(group_by)
        .agg(**state_growth.get_schema_agg(erc20_approvals_schema))
    )

    erc721_transfers_per_block = (
        df.filter((pl.col.topic0 == transfer) & (~pl.col.topic3.is_null()))
        .group_by(group_by)
        .agg(**state_growth.get_schema_agg(erc721_transfers_schema))
    )

    erc721_approvals_per_block = (
        df.filter((pl.col.topic0 == approval) & (~pl.col.topic3.is_null()))
        .group_by(group_by)
        .agg(**state_growth.get_schema_agg(erc721_approvals_schema))
    )

    return (
        df.group_by(group_by, maintain_order=True)
        .agg(**state_growth.get_schema_agg(schema))
        .join(erc20_transfers_per_block, on=group_by, how='outer_coalesce')
        .join(erc20_approvals_per_block, on=group_by, how='outer_coalesce')
        .join(erc721_transfers_per_block, on=group_by, how='outer_coalesce')
        .join(erc721_approvals_per_block, on=group_by, how='outer_coalesce')
    )

