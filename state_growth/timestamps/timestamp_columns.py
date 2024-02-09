from __future__ import annotations

import polars as pl


def add_time_column(
    dataset: pl.DataFrame,
    time_intervals: pl.DataFrame,
    time_column_name: str,
    *,
    clip: bool = True,
) -> pl.DataFrame:
    bounds = time_intervals['first_block'].to_list()
    bounds.append(time_intervals['last_block'][-1] + 1)
    labels = time_intervals[time_column_name].to_list()
    labels = ['PRE'] + labels + ['POST']

    # determine time period of each dataset row
    time_column = (
        dataset['block_number']
        .cut(bounds, labels=labels, left_closed=True)
        .cast(str)
        .alias(time_column_name)
    )

    # aggregate by time period
    return dataset.with_columns(time_column)


def add_missing_time_intervals(
    df: pl.DataFrame,
    time_intervals: pl.DataFrame,
    time_column_name: str,
) -> pl.DataFrame:
    return (
        df.join(
            time_intervals[[time_column_name]],
            on=time_column_name,
            how='outer_coalesce',
        )
        .fill_null(0)
        .sort(time_column_name)
    )
