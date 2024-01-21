from __future__ import annotations

import typing

import polars as pl


FrameType = typing.TypeVar('FrameType', pl.DataFrame, pl.LazyFrame)

binary_zero_word = b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'


event_types = {
    'transfer': '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
    'approval': '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925',
    'uniswap_sync': '0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1',
    'uniswap_swap': '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
    'transfer_single': '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
}

erc20s = {
    'weth': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    'usdt': '0xdac17f958d2ee523a2206206994597c13d831ec7',
    'usdc': '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
}


agg_path_template = '{timescale}/{datatype}/{filename}'
agg_filename_template = (
    '{network}__{datatype}__by_{timescale}__{from_time}_to_{to_time}.parquet'
)
agg_time_format = '%Y-%m-%d-%H%M%S'
