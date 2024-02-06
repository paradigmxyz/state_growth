from __future__ import annotations

import copy
import typing

import polars as pl

import state_growth

if typing.TYPE_CHECKING:
    from typing_extensions import Unpack, Callable
    from state_growth import FrameType, RawGlobKwargs
    from mypy_extensions import DefaultNamedArg

    GroupByArg = DefaultNamedArg(str, 'group_by')


def aggregate_dataset(
    df: FrameType, datatype: str, *, group_by: str = 'block_number'
) -> FrameType:
    return get_aggregate_function(datatype)(df)


def get_aggregate_function(
    datatype: str,
) -> Callable[[FrameType, GroupByArg], FrameType]:
    return {
        'balance_diffs': state_growth.aggregate_balance_diffs,
        'balance_reads': state_growth.aggregate_balance_reads,
        'contracts': state_growth.aggregate_contracts,
        'logs': state_growth.aggregate_logs,
        'storage_diffs': state_growth.aggregate_storage_diffs,
        'storage_reads': state_growth.aggregate_storage_reads,
        'transactions': state_growth.aggregate_transactions,
    }[datatype]


def load_multi_aggregate(
    *,
    datatypes: typing.Sequence[str] | None = None,
    **kwargs: Unpack[RawGlobKwargs],
) -> pl.DataFrame:
    if datatypes is None:
        datatypes = state_growth.all_datatypes

    dfs = {}
    for datatype in datatypes:
        datatype_kwargs = copy.copy(kwargs)
        datatype_kwargs['datatype'] = datatype
        dfs[datatype] = state_growth.load_and_aggregate(**datatype_kwargs)

    df = dfs[datatypes[0]]
    for datatype in datatypes[1:]:
        df = df.join(dfs[datatype], on='block_number', how='outer_coalesce')

    return df
