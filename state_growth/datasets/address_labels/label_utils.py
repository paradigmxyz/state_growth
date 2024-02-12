from __future__ import annotations

import os
import shutil
import typing

import polars as pl

import state_growth
from mypy_extensions import NamedArg
from . import manual_labels


Extractor = typing.Callable[
    [
        NamedArg(int, 'min_block'),
        NamedArg(int, 'max_block'),
        NamedArg(str, 'data_root'),
        NamedArg(str, 'network'),
    ],
    pl.DataFrame,
]


def load_address_labels(
    label_type: str | typing.Sequence[str],
    *,
    data_root: str,
    network: str = 'ethereum',
) -> pl.DataFrame:
    if isinstance(label_type, list) and len(label_type) > 0:
        dfs = [
            load_address_labels(
                sublabel_type,
                data_root=data_root,
                network=network,
            )
            for sublabel_type in label_type
        ]
        return pl.concat(dfs)

    elif isinstance(label_type, str):
        if label_type == 'manual':
            return manual_labels.load_manual_labels()
        else:
            path = state_growth.path_template.format(
                datatype=label_type,
                start_block='*' * 8,
                end_block='*' * 8,
                data_root=data_root,
                network=network,
            ).replace('********', '*')
            return pl.read_parquet(path)
    else:
        raise Exception('invalid label_type')


def extract_labels(
    datatype: str,
    min_block: int,
    max_block: int,
    *,
    data_root: str,
    network: str,
    overwrite: bool = False,
    f: Extractor,
) -> None:
    # create path
    path = state_growth.path_template.format(
        datatype=datatype,
        start_block=min_block,
        end_block=max_block,
        data_root=data_root,
        network=network,
    )
    if os.path.exists(path) and not overwrite:
        print('path already exists, skipping', path)
        return
    else:
        print('extracting data for', path)

    # extract data
    df = f(
        min_block=min_block,
        max_block=max_block,
        data_root=data_root,
        network=network,
    )

    # save output
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + '_tmp'
    df.write_parquet(tmp_path)
    shutil.move(tmp_path, path)
