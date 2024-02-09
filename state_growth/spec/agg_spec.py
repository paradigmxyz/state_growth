from __future__ import annotations

from decimal import Decimal
from typing import Union, Literal, Any, TypedDict, Mapping
from datetime import date, time, timedelta
import datetime
from polars import Expr, Series


PolarsAgg = Union[
    Union[
        Union[int, float, Decimal],
        Union[date, time, datetime.datetime, timedelta],
        str,
        bool,
        bytes,
        list[Any],
    ],
    Union[Expr, Series, str],
    None,
]


class AggColumn(TypedDict):
    type: Literal['count', 'unique', 'rate']
    agg: PolarsAgg


class AggSchema(TypedDict):
    columns: Mapping[str, AggColumn]


agg_path_template = '{timescale}/{datatype}/{filename}'
agg_filename_template = (
    '{network}__{datatype}__by_{timescale}__{from_time}_to_{to_time}.parquet'
)
agg_time_format = '%Y-%m-%d-%H%M%S'
