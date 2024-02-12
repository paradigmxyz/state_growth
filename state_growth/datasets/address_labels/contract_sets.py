from __future__ import annotations

import copy
from typing_extensions import Unpack

import polars as pl

import state_growth


def extract_erc20s(
    **kwargs: Unpack[state_growth.RawGlobKwargs],
) -> pl.DataFrame:
    kwargs = copy.copy(kwargs)
    kwargs['datatype'] = 'erc20_transfers'
    return (
        state_growth.scan_raw_dataset(
            columns=['block_number', 'erc20'],
            **kwargs,
        )
        .select(pl.col.erc20.unique())
        .rename({'erc20': 'address'})
        .with_columns(label=pl.lit('erc20'))
        .collect()
    )


def extract_erc721s(
    **kwargs: Unpack[state_growth.RawGlobKwargs],
) -> pl.DataFrame:
    kwargs = copy.copy(kwargs)
    kwargs['datatype'] = 'erc721_transfers'
    return (
        state_growth.scan_raw_dataset(
            columns=['block_number', 'erc20'],
            **kwargs,
        )
        .select(pl.col.erc20.unique())
        .rename({'erc20': 'address'})
        .with_columns(label=pl.lit('erc721'))
        .collect()
    )


def extract_erc1155s(
    **kwargs: Unpack[state_growth.RawGlobKwargs],
) -> pl.DataFrame:
    kwargs = copy.copy(kwargs)
    kwargs['datatype'] = 'logs'
    return (
        state_growth.scan_raw_dataset(
            columns=['block_number', 'topic0', 'address'],
            **kwargs,
        )
        .filter(
            (
                pl.col.topic0
                == bytes.fromhex(state_growth.event_types['TransferSingle'][2:])
            )
        )
        .select(pl.col.address.unique())
        .with_columns(label=pl.lit('erc1155'))
        .collect()
    )


def extract_chainlink_feeds(
    **kwargs: Unpack[state_growth.RawGlobKwargs],
) -> pl.DataFrame:
    kwargs = copy.copy(kwargs)
    kwargs['datatype'] = 'logs'
    return (
        state_growth.scan_raw_dataset(
            columns=['block_number', 'topic0', 'address'],
            **kwargs,
        )
        .filter(
            (
                pl.col.topic0
                == bytes.fromhex(state_growth.event_types['AnswerUpdated'][2:])
            )
            | (
                pl.col.topic0
                == bytes.fromhex(state_growth.event_types['AnswerUpdated_OLD'][2:])
            )
        )
        .select(pl.col.address.unique())
        .with_columns(label=pl.lit('chainlink_feed'))
        .collect()
    )
