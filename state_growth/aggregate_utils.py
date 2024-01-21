from __future__ import annotations

import typing

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
