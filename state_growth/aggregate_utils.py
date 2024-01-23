from __future__ import annotations

import typing

import polars as pl

import state_growth

if typing.TYPE_CHECKING:
    from state_growth import FrameType
    from mypy_extensions import DefaultNamedArg


def aggregate_dataset(
    df: FrameType, datatype: str, *, group_by: str = 'block_number'
) -> FrameType:
    return get_aggregate_function(datatype)(df)


def get_aggregate_function(
    datatype: str
) -> typing.Callable[[FrameType, DefaultNamedArg(str, 'group_by')], FrameType]:
    return {
        'balance_diffs': state_growth.aggregate_balance_diffs,
        'balance_reads': state_growth.aggregate_balance_reads,
        'logs': state_growth.aggregate_logs,
        'storage_diffs': state_growth.aggregate_storage_diffs,
        'storage_reads': state_growth.aggregate_storage_reads,
        'transactions': state_growth.aggregate_transactions,
    }[datatype]


def load_multi_aggregate(
    data_root: str, *, datatypes: typing.Sequence[str] | None = None
) -> pl.DataFrame:
    if datatypes is None:
        datatypes = state_growth.all_datatypes

    dfs = {}
    for datatype in datatypes:
        dfs[datatype] = state_growth.load_and_aggregate(datatype, data_root=data_root)

    df = dfs[datatypes[0]]
    for datatype in datatypes[1:]:
        df = df.join(dfs[datatype], on='block_number', how='outer_coalesce')

    return df
