from __future__ import annotations

import os
import typing

from typing_extensions import Unpack, NotRequired

import polars as pl

from state_growth import aggregate_dataset


class RawGlobKwargs(typing.TypedDict):
    data_root: str
    datatype: str
    min_block: NotRequired[int | None]
    max_block: NotRequired[int | None]
    network: NotRequired[str]


def get_raw_glob(
    data_root: str,
    datatype: str,
    *,
    min_block: int | None = None,
    max_block: int | None = None,
    network: str = 'ethereum',
) -> str:
    if min_block is not None and max_block is not None:
        prefix = common_prefix('%08d' % min_block, '%08d' % max_block)
        wildcard = '__' + prefix + '*.parquet'
    else:
        wildcard = '__*.parquet'

    return os.path.join(
        data_root, network, datatype, network + '__' + datatype + wildcard
    )


def scan_dataset(
    *,
    columns: typing.Sequence[str] | None = None,
    **kwargs: Unpack[RawGlobKwargs],
) -> pl.LazyFrame:
    scan = pl.scan_parquet(get_raw_glob(**kwargs))

    if columns is not None:
        scan = scan.select(columns)

    if kwargs.get('min_block') is not None:
        scan = scan.filter(pl.col.block_number >= kwargs['min_block'])
    if kwargs.get('max_block') is not None:
        scan = scan.filter(pl.col.block_number <= kwargs['max_block'])
    return scan


def load_dataset(
    *,
    columns: typing.Sequence[str] | None = None,
    **kwargs: Unpack[RawGlobKwargs],
) -> pl.DataFrame:
    return scan_dataset(columns=columns, **kwargs).collect()


def load_and_aggregate(**kwargs: Unpack[RawGlobKwargs]) -> pl.DataFrame:
    datatype = kwargs['datatype']
    scan = scan_dataset(**kwargs)
    return aggregate_dataset(scan, datatype).collect()


def common_prefix(str1: str, str2: str) -> str:
    min_length = min(len(str1), len(str2))
    for i in range(min_length):
        if str1[i] != str2[i]:
            return str1[:i]
    return str1[:min_length]

