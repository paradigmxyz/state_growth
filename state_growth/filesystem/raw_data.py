from __future__ import annotations

import os
import typing

from typing_extensions import Unpack

import polars as pl

from state_growth import aggregate_dataset


class RawGlobKwargs(typing.TypedDict, total=False):
    min_block: int | None
    max_block: int | None
    network: str


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

    return os.path.join(data_root, datatype, network + '__' + datatype + wildcard)


def scan_dataset(
    datatype: str,
    data_root: str,
    min_block: int | None = None,
    max_block: int | None = None,
    network: str = 'ethereum',
) -> pl.LazyFrame:
    data_glob = get_raw_glob(
        datatype=datatype,
        data_root=data_root,
        min_block=min_block,
        max_block=max_block,
        network=network,
    )
    scan = pl.scan_parquet(data_glob)
    if min_block is not None:
        scan = scan.filter(pl.col.block_number >= min_block)
    if max_block is not None:
        scan = scan.filter(pl.col.block_number <= max_block)
    return scan


def load_dataset(
    datatype: str, data_root: str, **kwargs: Unpack[RawGlobKwargs]
) -> pl.DataFrame:
    return scan_dataset(datatype, data_root, **kwargs).collect()


def load_and_aggregate(
    datatype: str, data_root: str, **kwargs: Unpack[RawGlobKwargs]
) -> pl.DataFrame:
    scan = scan_dataset(datatype, data_root, **kwargs)
    return aggregate_dataset(scan, datatype).collect()


def common_prefix(str1: str, str2: str) -> str:
    min_length = min(len(str1), len(str2))
    for i in range(min_length):
        if str1[i] != str2[i]:
            return str1[:i]
    return str1[:min_length]
