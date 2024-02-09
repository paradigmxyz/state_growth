from __future__ import annotations

import typing

import tooltime


Timescale = typing.Literal['block', 'hour', 'date', 'month', 'year']

time_columns_to_timelengths: typing.Mapping[BlockTimeColumn, tooltime.Timelength] = {
    'block_hour': '1h',
    'block_date': '1d',
    'block_month': '1M',
    'block_year': '1y',
}

BlockTimeColumn = typing.Literal[
    'block_number',
    'block_hour',
    'block_date',
    'block_month',
    'block_year',
]

interval_type_names: typing.Mapping[BlockTimeColumn, Timescale] = {
    'block_number': 'block',
    'block_hour': 'hour',
    'block_date': 'date',
    'block_month': 'month',
    'block_year': 'year',
}
