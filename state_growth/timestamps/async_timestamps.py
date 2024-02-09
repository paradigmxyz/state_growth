from __future__ import annotations

import asyncio

import ctc
import polars as pl
import tooltime


async def create_timestamp_column(
    df: pl.DataFrame,
    timescale: tooltime.Timelength,
    *,
    block_column: str = 'block_number',
) -> pl.Series:
    alias = {
        '1h': 'hour',
        3600: 'hour',
        '1d': 'date',
        86400: 'date',
        '1M': 'month',
        '1y': 'year',
    }.get(timescale, 'time')

    if len(df) == 0:
        return pl.Series(alias, []).cast(str)

    min_block = int(df[block_column].min())  # type: ignore  # noqa
    max_block = int(df[block_column].max())  # type: ignore  # noqa
    start_intervals_task = ctc.async_get_block_intervals(
        start_block=min_block,
        end_block=max_block,
        interval_size=timescale,
        blocks_at='start',
    )
    end_intervals_task = ctc.async_get_block_intervals(
        start_block=min_block,
        end_block=max_block,
        interval_size=timescale,
        blocks_at='end',
    )
    start_intervals, end_intervals = await asyncio.gather(
        start_intervals_task,
        end_intervals_task,
    )

    block_numbers = list(start_intervals['block'])
    block_numbers.append(end_intervals['block'][-1] + 1)
    labels = ['PRE_START'] + start_intervals['label'].to_list() + ['POST_END']

    return df[block_column].cut(block_numbers, labels=labels).alias(alias)


async def add_timestamp_column(
    df: pl.DataFrame,
    timescale: tooltime.Timelength,
    *,
    block_column: str = 'block_number',
) -> pl.DataFrame:
    column = await create_timestamp_column(df, timescale, block_column=block_column)
    df = df.clone()
    index = df.columns.index(block_column)
    df.insert_column(1 + index, column)
    return df


async def add_hour_column(
    df: pl.DataFrame,
    *,
    block_column: str = 'block_number',
) -> pl.DataFrame:
    return await add_timestamp_column(df, '1h', block_column=block_column)


async def add_date_column(
    df: pl.DataFrame,
    *,
    block_column: str = 'block_number',
) -> pl.DataFrame:
    return await add_timestamp_column(df, '1d', block_column=block_column)


async def add_month_column(
    df: pl.DataFrame,
    *,
    block_column: str = 'block_number',
) -> pl.DataFrame:
    return await add_timestamp_column(df, '1M', block_column=block_column)


async def add_year_column(
    df: pl.DataFrame,
    *,
    block_column: str = 'block_number',
) -> pl.DataFrame:
    return await add_timestamp_column(df, '1y', block_column=block_column)
