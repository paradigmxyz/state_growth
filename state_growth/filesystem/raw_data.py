from __future__ import annotations

import os

import polars as pl

from state_growth import aggregate_dataset


def get_raw_glob(
    data_root: str,
    datatype: str,
    *,
    min_block: int | None = None,
    max_block: int | None = None,
    network: str = 'ethereum',
) -> str:
    return os.path.join(data_root, datatype, network + '__' + datatype + '__*.parquet')


def scan_dataset(datatype: str, data_root: str) -> pl.LazyFrame:
    data_glob = get_raw_glob(datatype=datatype, data_root=data_root)
    return pl.scan_parquet(data_glob)


def load_dataset(datatype: str, data_root: str) -> pl.DataFrame:
    return scan_dataset(datatype, data_root).collect()


def load_and_aggregate(datatype: str, data_root: str) -> pl.DataFrame:
    return aggregate_dataset(scan_dataset(datatype, data_root), datatype).collect()
