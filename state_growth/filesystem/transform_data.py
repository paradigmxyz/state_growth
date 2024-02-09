from __future__ import annotations

from typing import Sequence
from typing_extensions import Unpack
import polars as pl

import state_growth


def get_transformed_glob(**kwargs: Unpack[state_growth.RawGlobKwargs]) -> str:
    return state_growth.get_glob(meta_datatype='transformed', **kwargs)


def scan_transformed_dataset(
    *,
    columns: Sequence[str] | None = None,
    **kwargs: Unpack[state_growth.RawGlobKwargs],
) -> pl.LazyFrame:
    return state_growth.scan_dataset(
        columns=columns, meta_datatype='transformed', **kwargs
    )


def load_transformed_dataset(
    *,
    columns: Sequence[str] | None = None,
    **kwargs: Unpack[state_growth.RawGlobKwargs],
) -> pl.DataFrame:
    return state_growth.scan_dataset(
        columns=columns, meta_datatype='transformed', **kwargs
    ).collect()
