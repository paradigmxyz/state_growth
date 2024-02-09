from __future__ import annotations

import typing
from typing_extensions import Unpack

import polars as pl

import state_growth


def get_raw_glob(**kwargs: Unpack[state_growth.RawGlobKwargs]) -> str:
    return state_growth.get_glob(meta_datatype='raw', **kwargs)


def scan_raw_dataset(
    *,
    columns: typing.Sequence[str] | None = None,
    **kwargs: Unpack[state_growth.RawGlobKwargs],
) -> pl.LazyFrame:
    return state_growth.scan_dataset(columns=columns, meta_datatype='raw', **kwargs)


def load_raw_dataset(
    *,
    columns: typing.Sequence[str] | None = None,
    **kwargs: Unpack[state_growth.RawGlobKwargs],
) -> pl.DataFrame:
    result = state_growth.scan_dataset(
        columns=columns, meta_datatype='raw', **kwargs
    ).collect()
    return result
