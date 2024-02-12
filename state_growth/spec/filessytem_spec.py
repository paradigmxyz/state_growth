from __future__ import annotations

import os
import typing
from typing_extensions import NotRequired


filename_template = '{network}__{datatype}__{start_block:08}_to_{end_block:08}.parquet'
dir_template = '{data_root}/{network}_state_growth/address_labels'
path_template = os.path.join(dir_template, filename_template)


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
