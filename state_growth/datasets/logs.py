from __future__ import annotations

import polars as pl

from state_growth.spec import event_types, erc20s


def aggregate_logs(df: pl.DataFrame) -> pl.DataFrame:
    erc20_transfers_per_block = (
        df.filter(
            (pl.col.topic0 == bytes.fromhex(event_types['transfer'][2:]))
            & (pl.col.topic3.is_null())
        )
        .group_by('block_number')
        .agg(
            erc20_transfers=pl.len(),
            weth_transfers=pl.col.address.filter(
                pl.col.address == bytes.fromhex(erc20s['weth'][2:])
            ).count(),
            usdt_transfers=pl.col.address.filter(
                pl.col.address == bytes.fromhex(erc20s['usdt'][2:])
            ).count(),
            usdc_transfers=pl.col.address.filter(
                pl.col.address == bytes.fromhex(erc20s['usdc'][2:])
            ).count(),
        )
    )

    erc20_approvals_per_block = (
        df.filter(
            (pl.col.topic0 == bytes.fromhex(event_types['approval'][2:]))
            & (pl.col.topic3.is_null())
        )
        .group_by('block_number')
        .agg(
            erc20_approvals=pl.len(),
            weth_approvals=pl.col.address.filter(
                pl.col.address == bytes.fromhex(erc20s['weth'][2:])
            ).count(),
            usdt_approvals=pl.col.address.filter(
                pl.col.address == bytes.fromhex(erc20s['usdt'][2:])
            ).count(),
            usdc_approvals=pl.col.address.filter(
                pl.col.address == bytes.fromhex(erc20s['usdc'][2:])
            ).count(),
        )
    )

    erc721_transfers_per_block = (
        df.filter(
            (pl.col.topic0 == bytes.fromhex(event_types['transfer'][2:]))
            & (~pl.col.topic3.is_null())
        )
        .group_by('block_number')
        .agg(
            erc721_transfers=pl.len(),
        )
    )

    erc721_approvals_per_block = (
        df.filter(
            (pl.col.topic0 == bytes.fromhex(event_types['approval'][2:]))
            & (~pl.col.topic3.is_null())
        )
        .group_by('block_number')
        .agg(
            erc721_approvals=pl.len(),
        )
    )

    return df.group_by('block_number', maintain_order=True).agg(
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
    ).join(
        erc20_transfers_per_block,
        on='block_number',
        how='outer_coalesce',
    ).join(
        erc20_approvals_per_block,
        on='block_number',
        how='outer_coalesce',
    ).join(
        erc721_transfers_per_block,
        on='block_number',
        how='outer_coalesce',
    ).join(
        erc721_approvals_per_block,
        on='block_number',
        how='outer_coalesce',
    )
