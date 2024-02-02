from typing_extensions import Unpack, Literal, TypedDict

import matplotlib.pyplot as plt
import polars as pl
import toolplot


TimeInterval = Literal['block', '1h', '1d', '1M', '1y']


class XTickKwargs(TypedDict, total=False):
    pass


def get_time_column(interval: TimeInterval):
    return {
        'block': 'block_number',
        '1h': 'block_hour',
        '1d': 'block_date',
        '1M': 'block_month',
        '1y': 'block_year',
    }[interval]


def detect_interval(df: pl.DataFrame) -> TimeInterval:
    possible_columns = [
        'block_number',
        'block_hour',
        'block_date',
        'block_month',
        'block_year',
    ]
    contained = [column for column in df.columns if column in possible_columns]
    if len(contained) == 1:
        return {
            'block_number': 'block',
            'block_hour': '1h',
            'block_date': '1d',
            'block_month': '1M',
            'block_year': '1y',
        }[contained[0]]
    elif len(contained) == 0:
        raise Exception('could not detect interval, no time columns present')
    else:
        raise Exception('could not decide interval, multiple time columns present')


def set_plot_xticks(
    df: pl.DataFrame,
    interval: TimeInterval,
    *,
    offset: int | None = None,
    step_size: int | None = None,
) -> None:
    if offset is None:
        offset = 0
    if step_size is None:
        step_size = {
            'block': int(len(df) / 5),
            '1h': 24,
            '1d': 365,
            '1M': 12,
            '1y': 1,
        }
    time_slice = slice(offset, None, step_size)

    time_column = get_time_column(interval)
    plt.xticks(list(range(len(df)))[time_slice], df[time_column][time_slice])


def plot_raw_agg_columns(
    df: pl.DataFrame,
    interval: TimeInterval | None = None,
    **xtick_kwargs: Unpack[XTickKwargs],
) -> None:
    if interval is None:
        interval = detect_interval(df)

    title_prefix = {
        'block': 'per-block ',
        '1h': 'hourly ',
        '1d': 'daily ',
        '1M': 'monthly ',
        '1y': 'yearly ',
    }[interval]
    time_column = get_time_column(interval)

    for column in df.columns[1:]:
        if column == time_column:
            continue
        print(column)
        plt.plot(df[column])
        plt.title(title_prefix + column)
        set_plot_xticks(df, interval, **xtick_kwargs)
        toolplot.format_yticks()
        toolplot.add_tick_grid()
        plt.show()

