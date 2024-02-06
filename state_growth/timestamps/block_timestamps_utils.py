from __future__ import annotations

import copy
import typing

import polars as pl
import tooltime
from .. import filesystem

if typing.TYPE_CHECKING:
    Timescale = typing.Literal['hour', 'date', 'month', 'year']

    from ..filesystem import RawGlobKwargs
    from typing_extensions import Sequence, Unpack


def load_block_timestamps(**kwargs: Unpack[RawGlobKwargs]) -> pl.DataFrame:
    glob_kwargs = copy.copy(kwargs)
    glob_kwargs['datatype'] = 'blocks'
    # blocks_glob = filesystem.get_raw_glob(**glob_kwargs)

    # block_timestamps = (
    #     pl.scan_parquet(blocks_glob).select('block_number', 'timestamp').collect()
    # )
    block_timestamps = filesystem.scan_dataset(**glob_kwargs).select('block_number', 'timestamp').collect()
    block_timestamps = block_timestamps.with_columns(
        pl.col.block_number.set_sorted(),
        pl.col.timestamp.set_sorted(),
    )

    # fix any zero timestamps
    if block_timestamps['timestamp'].min() == 0:
        min_timestamp = (
            block_timestamps[['timestamp']].filter(pl.col.timestamp > 0).min()
        )
        block_timestamps = block_timestamps.with_columns(
            pl.when(pl.col.timestamp > 0)
            .then(pl.col.timestamp)
            .otherwise(min_timestamp - 100)
        )

    return block_timestamps


def get_block_hours(block_timestamps: pl.DataFrame) -> pl.DataFrame:
    intervals = get_block_time_intervals(block_timestamps, ['block_hour'])
    return intervals['block_hour']


def get_block_dates(block_timestamps: pl.DataFrame) -> pl.DataFrame:
    intervals = get_block_time_intervals(block_timestamps, ['block_date'])
    return intervals['block_date']


def get_block_months(block_timestamps: pl.DataFrame) -> pl.DataFrame:
    intervals = get_block_time_intervals(block_timestamps, ['block_month'])
    return intervals['block_month']


def get_block_years(block_timestamps: pl.DataFrame) -> pl.DataFrame:
    intervals = get_block_time_intervals(block_timestamps, ['block_year'])
    return intervals['block_year']


def get_block_time_intervals(
    block_timestamps: pl.DataFrame,
    timescales: Sequence[Timescale] | None = None,
) -> pl.DataFrame:
    """convert block timestamps"""

    time_columns = {
        'block_hour': '1h',
        'block_date': '1d',
        'block_month': '1M',
        'block_year': '1y',
    }

    if timescales is None:
        timescales = list(time_columns.keys())

    # add intervals
    ethereum_genesis = '2015-07'
    now = tooltime.now()
    time_bounds = {}
    for column_name in timescales:
        interval_size = time_columns[column_name]

        # get intervals for given timescale
        intervals = tooltime.get_interval_df(
            start_time=ethereum_genesis,
            end_time=now,
            interval_size=interval_size,
        )

        # reformat block hour
        if column_name == 'block_hour':
            intervals = intervals.with_columns(
                pl.col.label.str.replace(' ', '_').str.replace(':', '-')
            )
        column = (
            block_timestamps['timestamp']
            .cut(
                intervals['start_timestamp'],
                labels=['PRE'] + intervals['label'].to_list(),
            )
            .alias(column_name)
        )
        block_timestamps = block_timestamps.with_columns(column)

        time_bounds[column_name] = (
            block_timestamps.group_by(column_name)
            .agg(
                first_block=pl.col.block_number.first(),
                last_block=pl.col.block_number.last(),
            )
            .sort(column_name)
            .with_columns(n_blocks=pl.col.last_block - pl.col.first_block + 1)
        )

    return time_bounds
