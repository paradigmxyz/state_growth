from __future__ import annotations

import os
import typing
from typing_extensions import Unpack

import polars as pl

import state_growth


def get_glob(
    *,
    data_root: str,
    meta_datatype: state_growth.MetaDatatype,
    datatype: str,
    network: str = 'ethereum',
    min_block: int | None = None,
    max_block: int | None = None,
) -> str:
    # get data category dir
    if meta_datatype == 'raw':
        meta_dir = network
    elif meta_datatype == 'transformed':
        meta_dir = network + '_state_growth'

    # create wildcard
    if min_block is not None and max_block is not None:
        prefix = common_prefix('%08d' % min_block, '%08d' % max_block)
        wildcard = '__' + prefix + '*.parquet'
    else:
        wildcard = '__*.parquet'

    # create path
    return os.path.join(
        data_root,
        meta_dir,
        datatype,
        network + '__' + datatype + wildcard,
    )


def scan_dataset(
    *,
    columns: typing.Sequence[str] | None = None,
    **kwargs: Unpack[state_growth.GlobKwargs],
) -> pl.LazyFrame:
    file_glob = get_glob(**kwargs)
    scan = pl.scan_parquet(file_glob)

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
    **kwargs: Unpack[state_growth.GlobKwargs],
) -> pl.DataFrame:
    return scan_dataset(columns=columns, **kwargs).collect()


def common_prefix(str1: str, str2: str) -> str:
    min_length = min(len(str1), len(str2))
    for i in range(min_length):
        if str1[i] != str2[i]:
            return str1[:i]
    return str1[:min_length]
