from __future__ import annotations

import typing
from typing_extensions import NotRequired


MetaDatatype = typing.Literal['raw', 'transformed']


class GlobKwargs(typing.TypedDict):
    data_root: str
    datatype: str
    meta_datatype: NotRequired[MetaDatatype]
    network: NotRequired[str]
    min_block: NotRequired[int | None]
    max_block: NotRequired[int | None]


class RawGlobKwargs(typing.TypedDict):
    data_root: str
    datatype: str
    min_block: NotRequired[int | None]
    max_block: NotRequired[int | None]
    network: NotRequired[str]
