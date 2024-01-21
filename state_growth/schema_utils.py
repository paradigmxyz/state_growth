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


def get_schema_agg(schema: AggSchema) -> Mapping[str, PolarsAgg]:
    return {k: v['agg'] for k, v in schema['columns'].items()}
