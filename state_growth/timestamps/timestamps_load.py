from __future__ import annotations

import typing

import polars as pl

import state_growth

if typing.TYPE_CHECKING:
    Timescale = typing.Literal['hour', 'date', 'month', 'year']


def load_block_timestamps(
    *,
    data_root: str,
    network: str = 'ethereum',
    min_block: int | None = None,
    max_block: int | None = None,
) -> pl.DataFrame:
    block_timestamps = state_growth.load_raw_dataset(
        datatype='blocks',
        data_root=data_root,
        network=network,
        min_block=min_block,
        max_block=max_block,
        columns=['block_number', 'timestamp'],
    )
    block_timestamps = block_timestamps.with_columns(
        pl.col.block_number.set_sorted(),
        pl.col.timestamp.set_sorted(),
    )

    # fix any zero timestamps
    if block_timestamps['timestamp'].min() == 0:
        min_timestamp = float(
            block_timestamps[['timestamp']]
            .filter(pl.col.timestamp > 0)
            .min()['timestamp'][0]
        )
        block_timestamps = block_timestamps.with_columns(
            pl.when(pl.col.timestamp > 0)
            .then(pl.col.timestamp)
            .otherwise(min_timestamp - 100)
        )

    return block_timestamps

