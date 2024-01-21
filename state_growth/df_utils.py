from __future__ import annotations

import typing

import polars as pl


def fill_missing_rows(
    df: pl.DataFrame,
    index_col: str,
    min_value: int | None = None,
    max_value: int | None = None,
) -> pl.DataFrame:
    if min_value is None:
        min_value = typing.cast(int, df[index_col].min())
    if max_value is None:
        max_value = typing.cast(int, df[index_col].max())
    full = pl.DataFrame(pl.Series(index_col, range(min_value, max_value), pl.UInt32))
    return df.join(full, on=index_col, how='outer_coalesce').fill_null(0)
