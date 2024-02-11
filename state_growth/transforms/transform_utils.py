from __future__ import annotations

import copy
import os
import typing
import shutil

import polars as pl
import numpy as np
from mypy_extensions import NamedArg
from typing_extensions import Callable

import state_growth

Transformer = Callable[
    [
        NamedArg(pl.DataFrame, 'raw_dataset'),
        NamedArg(pl.DataFrame, 'time_intervals'),
        NamedArg(str, 'time_column_name'),
        NamedArg(typing.Mapping[str, typing.Any], 'extras'),
    ],
    pl.DataFrame,
]


def transform_chunks(
    raw_input_datatype: str,
    output_datatype_template: str,
    extras: typing.Mapping[str, typing.Any],
    join_time_column: bool,
    f: Transformer,
    time_columns: typing.Sequence[state_growth.BlockTimeColumn],
    context: state_growth.DataContext,
    min_block: int,
    max_block: int,
    chunk_size: int,
    add_missing_time_intervals: bool = True,
    verbose: bool = False,
) -> None:
    # decide chunks
    chunk_bounds = np.arange(min_block, max_block + chunk_size, chunk_size)
    chunks = []
    for start_block, end_block in zip(chunk_bounds[:-1], chunk_bounds[1:]):
        chunk = (start_block, end_block - 1)
        chunks.append(chunk)

    if verbose:
        print('transforming', len(chunks), 'chunks:')
        for chunk in chunks:
            print('-', chunk)
        print()

    block_timestamps = state_growth.load_block_timestamps(**context)

    for interval_type in time_columns:
        output_datatype = output_datatype_template.format(
            interval=state_growth.interval_type_names[interval_type]
        )
        time_column_name = interval_type
        raw_time_data = state_growth.get_block_time_interval(
            block_timestamps, state_growth.interval_type_names[interval_type]
        )

        for start_block, end_block in chunks:
            transform_chunk(
                raw_input_datatype=raw_input_datatype,
                extras=extras,
                join_time_column=join_time_column,
                f=f,
                context=context,
                output_datatype=output_datatype,
                time_column_name=time_column_name,
                raw_time_data=raw_time_data,
                start_block=start_block,
                end_block=end_block,
                verbose=verbose,
            )


def transform_chunk(
    raw_input_datatype: str,
    extras: typing.Mapping[str, typing.Any],
    join_time_column: bool,
    f: Transformer,
    context: state_growth.DataContext,
    output_datatype: str,
    time_column_name: str,
    raw_time_data: pl.DataFrame,
    start_block: int,
    end_block: int,
    add_missing_time_intervals: bool = True,
    verbose: bool = False,
) -> None:
    # collect all data starting at pre interval
    pre_interval = raw_time_data.filter(pl.col.first_block < start_block)[-1]
    if len(pre_interval) != 0:
        chunk_start = pre_interval['first_block'][0]
    else:
        chunk_start = 0

    # collect all data from
    time_intervals = raw_time_data.filter(
        (pl.col.first_block >= chunk_start) & (pl.col.first_block <= end_block)
    ).with_columns(pl.col(time_column_name).cast(str))

    # collect all data up to final interval
    chunk_end = time_intervals['last_block'][-1]

    # get output path
    parent_dir = os.path.join(
        context['data_root'],
        context['network'] + '_' + 'state_growth',
        output_datatype,
    )
    filename = '{network}__{datatype}__{start_block:08}_to_{end_block:08}.parquet'.format(
        network=context['network'],
        datatype=output_datatype,
        start_block=start_block,
        end_block=end_block,
    )
    os.makedirs(parent_dir, exist_ok=True)
    path = os.path.join(parent_dir, filename)

    # skip if file already exists
    if os.path.exists(path):
        print('skipping', filename)
        return
    else:
        print('doing   ', filename)
        if verbose:
            print(
                '    for chunk',
                (start_block, end_block),
                'using total bounds',
                (chunk_start, chunk_end),
            )

    # load relevant data
    chunk_context = copy.copy(context)
    raw_dataset = state_growth.load_raw_dataset(
        datatype=raw_input_datatype,
        min_block=chunk_start,
        max_block=chunk_end,
        **chunk_context,
    )

    if join_time_column and time_column_name != 'block_number':
        raw_dataset = state_growth.add_time_column(
            raw_dataset,
            time_intervals=time_intervals,
            time_column_name=time_column_name,
        )

    # get processed dataset
    transformed = f(
        raw_dataset=raw_dataset,
        time_intervals=time_intervals,
        time_column_name=time_column_name,
        extras=extras,
    )

    # convert u32 to u64 to avoid future overflows
    transformed = transformed.with_columns(pl.col(pl.UInt32).cast(pl.UInt64))

    # add missing time intervals
    if add_missing_time_intervals:
        transformed = state_growth.add_missing_time_intervals(
            transformed,
            time_intervals=time_intervals,
            time_column_name=time_column_name,
        )

    # add block bounds
    transformed = transformed.join(
        time_intervals[[time_column_name, 'first_block', 'last_block']],
        on=time_column_name,
        how='left',
    )
    columns = [time_column_name, 'first_block', 'last_block']
    columns = columns + [
        column for column in transformed.columns if column not in columns
    ]
    transformed = transformed.select(columns)

    # filter out pre-intervals
    min_block = transformed['first_block'].min()
    if min_block is not None and typing.cast(int, min_block) < start_block:
        transformed = transformed.filter(pl.col.first_block >= start_block)

    # write file
    tmp_path = path + '_tmp'
    transformed.write_parquet(tmp_path)
    shutil.move(tmp_path, path)

