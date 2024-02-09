from __future__ import annotations

import polars as pl


def add_unique_per_interval(
    dataset: pl.DataFrame,
    time_data: pl.DataFrame,
    unique_column: str,
    time_column_name: str,
) -> pl.DataFrame:
    unique_per_interval = get_unique_per_interval(
        dataset=dataset,
        time_data=time_data,
        unique_column=unique_column,
        time_column_name=time_column_name,
    )
    column_order = time_data.columns + unique_per_interval.columns[1:]
    df = unique_per_interval.join(time_data, on=time_column_name)[column_order]
    return df


def get_unique_per_interval(
    dataset: pl.DataFrame,
    time_data: pl.DataFrame,
    unique_column: str,
    time_column_name: str,
) -> pl.DataFrame:
    unique_per_interval = get_unique_per_time_period(
        dataset, time_data, unique_column, time_column_name
    )
    unique_per_prev_union = get_unique_group_unions(
        dataset, time_data, unique_column, time_column_name
    )

    # add unique column per group
    unique_per_interval = unique_per_interval.filter(
        pl.col(time_column_name) != 'PRE'
    ).join(unique_per_prev_union, on=time_column_name, how='left')

    # add intersection column
    unique_per_interval = add_unique_intersection(unique_per_interval)

    return unique_per_interval


def get_unique_per_time_period(
    dataset: pl.DataFrame,
    time_data: pl.DataFrame,
    unique_column: str,
    time_column_name: str,
) -> pl.DataFrame:
    bounds = time_data['first_block'].to_list()
    labels = time_data[time_column_name].to_list()

    # determine time period of each dataset row
    time_column = (
        dataset['block_number']
        .cut(bounds, labels=['PRE'] + labels, left_closed=True)
        .cast(str)
        .alias(time_column_name)
    )

    # aggregate by time period
    df = (
        dataset.with_columns(time_column)
        .group_by(time_column_name, maintain_order=True)
        .agg(n_unique=pl.col(unique_column).n_unique())
    )

    # add missing time intervals
    df = add_missing_time_intervals(df, time_data, time_column_name)

    return df


def add_missing_time_intervals(
    df: pl.DataFrame,
    time_data: pl.DataFrame,
    time_column_name: str,
) -> pl.DataFrame:
    return (
        df.join(
            time_data[[time_column_name]],
            on=time_column_name,
            how='outer_coalesce',
        )
        .fill_null(0)
        .sort(time_column_name)
    )


def get_unique_group_unions(
    dataset: pl.DataFrame,
    time_data: pl.DataFrame,
    unique_column: str,
    time_column_name: str,
) -> pl.DataFrame:
    union_bounds_1 = time_data['first_block'][1::2].to_list()
    label_bounds_1 = time_data[time_column_name][::2].to_list()
    union_bounds_2 = time_data['first_block'][::2].to_list()
    label_bounds_2 = ['PRE'] + time_data[time_column_name][1::2].to_list()

    if len(time_data) % 2 == 1:
        label_bounds_2.append('POST')
    else:
        label_bounds_1.append('POST')

    # per even unions
    time_column_union_1 = (
        dataset['block_number']
        .cut(union_bounds_1, labels=label_bounds_1, left_closed=True)
        .cast(str)
        .alias(time_column_name)
    )
    unique_per_union_1 = (
        dataset.with_columns(time_column_union_1)
        .group_by(time_column_name, maintain_order=True)
        .agg(n_unique=pl.col(unique_column).n_unique())
    )

    # per odd unions
    time_column_union_2 = (
        dataset['block_number']
        .cut(union_bounds_2, labels=label_bounds_2, left_closed=True)
        .cast(str)
        .alias(time_column_name)
    )
    unique_per_union_2 = (
        dataset.with_columns(time_column_union_2)
        .group_by(time_column_name, maintain_order=True)
        .agg(n_unique=pl.col(unique_column).n_unique())
    )

    # join all union rows into single dataframe
    all_union_rows = (
        pl.concat([unique_per_union_1, unique_per_union_2])
        .sort(time_column_name)
        .rename({'n_unique': 'n_unique_prev_union'})
        .filter(pl.col(time_column_name) != 'PRE')
    )

    return all_union_rows


def add_unique_intersection(df: pl.DataFrame) -> pl.DataFrame:
    """
    (A union B) = A + B - (A intersection B)
    -> (A intersection B) = A + B - (A union B)
    """
    prev_union_expr = (
        df['n_unique'][1:] + df['n_unique'][:-1] - df['n_unique_prev_union'][1:]
    )
    overlap_expr = pl.col.n_unique_prev_intersection / pl.col.n_unique
    return (
        df[1:]
        .with_columns(n_unique_prev_intersection=prev_union_expr)
        .with_columns(overlap_with_previous=overlap_expr)
    )
