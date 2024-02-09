from __future__ import annotations

import typing
import polars as pl

import state_growth
from state_growth.datasets import io_per_contract


def f(
    *,
    raw_dataset: pl.DataFrame,
    time_intervals: pl.DataFrame,
    time_column_name: str,
    extras: typing.Mapping[str, typing.Any],
) -> pl.DataFrame:
    return io_per_contract.aggregate_contract_slot_diffs(
        df=raw_dataset,
        time_column=time_column_name,
    )


context: state_growth.DataContext = {
    'data_root': '/home/storm/data',
    'network': 'ethereum',
}
raw_input_datatype = 'storage_diffs'
output_datatype_template = 'writes_per_{interval}_contract'
join_time_column = True
min_block = 0
max_block = 18_999_999
chunk_size = 1_000_000
interval_types: list[state_growth.BlockTimeColumn] = [
    'block_number',
    'block_hour',
    'block_date',
    'block_month',
]
extras: dict[str, typing.Any] = {}


if __name__ == '__main__':
    state_growth.transform_chunks(
        raw_input_datatype=raw_input_datatype,
        output_datatype_template=output_datatype_template,
        extras=extras,
        join_time_column=join_time_column,
        f=f,
        time_columns=interval_types,
        context=context,
        min_block=min_block,
        max_block=max_block,
        chunk_size=chunk_size,
    )
