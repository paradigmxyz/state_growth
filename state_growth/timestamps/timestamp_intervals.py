from __future__ import annotations

import typing

import polars as pl
import tooltime

import state_growth

if typing.TYPE_CHECKING:
    from typing_extensions import Sequence


def get_block_time_interval(
    block_timestamps: pl.DataFrame,
    interval_type: state_growth.Timescale,
) -> pl.DataFrame:
    if interval_type == 'hour':
        return get_block_hours(block_timestamps)
    elif interval_type == 'date':
        return get_block_dates(block_timestamps)
    elif interval_type == 'month':
        return get_block_months(block_timestamps)
    elif interval_type == 'year':
        return get_block_years(block_timestamps)
    else:
        raise Exception('invalid interval type: ' + str(interval_type))


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
    time_columns: Sequence[state_growth.BlockTimeColumn] | None = None,
) -> typing.Mapping[str, pl.DataFrame]:
    """convert block timestamps"""

    if time_columns is None:
        time_columns = list(state_growth.time_columns_to_timelengths.keys())

    # add intervals
    ethereum_genesis = '2015-07'
    now = tooltime.now()
    time_bounds = {}
    for cn in time_columns:
        column_name = str(cn)
        interval_size = state_growth.time_columns_to_timelengths[cn]

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

        bounds = intervals['start_timestamp'].to_list()
        labels = ['PRE'] + intervals['label'].to_list()
        column = (
            block_timestamps['timestamp'].cut(bounds, labels=labels).alias(column_name)
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
