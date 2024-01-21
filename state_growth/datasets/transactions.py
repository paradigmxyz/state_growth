from __future__ import annotations

import typing

import polars as pl


def aggregate_transactions(
    txs: pl.DataFrame, min_block: int | None = None, max_block: int | None = None
) -> pl.DataFrame:
    txs_agg = (
        txs.group_by('block_number')
        .agg(
            n_txs=pl.len(),
            n_from_addresses=pl.col.from_address.n_unique(),
            n_to_addresses=pl.col.to_address.n_unique(),
            gas_used=pl.sum('gas_used'),
            n_input_bytes=pl.sum('n_input_bytes'),
            n_input_zero_bytes=pl.sum('n_input_zero_bytes'),
            n_input_nonzero_bytes=pl.sum('n_input_nonzero_bytes'),
            n_rlp_bytes=pl.sum('n_rlp_bytes'),
        )
        .sort('block_number')
    )

    if min_block is None:
        min_block = typing.cast(int, txs['block_number'].min())
    if max_block is None:
        max_block = typing.cast(int, txs['block_number'].max())
    df = pl.DataFrame(pl.Series('block_number', range(min_block, max_block), pl.UInt32))
    txs_agg = txs_agg.join(df, on='block_number', how='outer_coalesce').fill_null(0)

    return txs_agg
