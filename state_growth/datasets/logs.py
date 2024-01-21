from __future__ import annotations

import polars as pl

from state_growth.spec import event_types, erc20s, FrameType


def aggregate_logs(df: FrameType, *, group_by: str = 'block_number') -> FrameType:
    transfer = bytes.fromhex(event_types['transfer'][2:])
    approval = bytes.fromhex(event_types['approval'][2:])
    WETH = bytes.fromhex(erc20s['weth'][2:])
    USDT = bytes.fromhex(erc20s['usdt'][2:])
    USDC = bytes.fromhex(erc20s['usdc'][2:])

    erc20_transfers_per_block = (
        df.filter((pl.col.topic0 == transfer) & (pl.col.topic3.is_null()))
        .group_by(group_by)
        .agg(
            erc20_transfers=pl.len(),
            weth_transfers=pl.col.address.filter(pl.col.address == WETH).count(),
            usdt_transfers=pl.col.address.filter(pl.col.address == USDT).count(),
            usdc_transfers=pl.col.address.filter(pl.col.address == USDC).count(),
        )
    )

    erc20_approvals_per_block = (
        df.filter((pl.col.topic0 == approval) & (pl.col.topic3.is_null()))
        .group_by(group_by)
        .agg(
            erc20_approvals=pl.len(),
            weth_approvals=pl.col.address.filter(pl.col.address == WETH).count(),
            usdt_approvals=pl.col.address.filter(pl.col.address == USDT).count(),
            usdc_approvals=pl.col.address.filter(pl.col.address == USDC).count(),
        )
    )

    erc721_transfers_per_block = (
        df.filter((pl.col.topic0 == transfer) & (~pl.col.topic3.is_null()))
        .group_by(group_by)
        .agg(erc721_transfers=pl.len())
    )

    erc721_approvals_per_block = (
        df.filter((pl.col.topic0 == approval) & (~pl.col.topic3.is_null()))
        .group_by(group_by)
        .agg(erc721_approvals=pl.len())
    )

    return (
        df.group_by(group_by, maintain_order=True)
        .agg(
            n_logs=pl.len(),
            n_topics=(
                4 * pl.len()
                - pl.col('topic0').null_count()
                - pl.col('topic1').null_count()
                - pl.col('topic2').null_count()
                - pl.col('topic3').null_count()
            ),
            n_event_types=pl.col.topic0.n_unique(),
            n_log_data_bytes=pl.sum('n_data_bytes'),
        )
        .join(erc20_transfers_per_block, on=group_by, how='outer_coalesce')
        .join(erc20_approvals_per_block, on=group_by, how='outer_coalesce')
        .join(erc721_transfers_per_block, on=group_by, how='outer_coalesce')
        .join(erc721_approvals_per_block, on=group_by, how='outer_coalesce')
    )
