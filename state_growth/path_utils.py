import datetime
import typing
import os

import polars as pl
import tooltime


path_template = '{timescale}/{datatype}/{filename}'
filename_template = (
    '{network}__{datatype}__by_{timescale}__{from_time}_to_{to_time}.parquet'
)
time_format = '%Y-%m-%d-%H%M%S'


class ParsedFilename(typing.TypedDict):
    network: str
    datatype: str
    timescale: str
    from_time: str
    to_time: str


def timestamp_to_str(timestamp: tooltime.Timestamp) -> str:
    return tooltime.timestamp_to_datetime(timestamp).strftime(time_format)


def str_to_timestamp(raw_str: str) -> int:
    dt = datetime.datetime.strptime(raw_str, time_format)
    return tooltime.timestamp_to_seconds(dt)


def get_filename(
    network: str,
    datatype: str,
    timescale: str,
    from_time: tooltime.Timestamp,
    to_time: tooltime.Timestamp,
) -> str:
    return filename_template.format(
        network=network,
        datatype=datatype,
        timescale=timescale,
        from_time=timestamp_to_str(from_time),
        to_time=timestamp_to_str(to_time),
    )


def get_path(
    network: str,
    datatype: str,
    timescale: str,
    from_time: tooltime.Timestamp,
    to_time: tooltime.Timestamp,
    *,
    data_root: str,
) -> str:
    filename = get_filename(network, datatype, timescale, from_time, to_time)
    relpath = path_template.format(
        timescale=timescale, datatype=datatype, filename=filename
    )
    return os.path.join(data_root, relpath)


def parse_path(path: str) -> ParsedFilename:
    filename = os.path.basename(path)
    try:
        network, datatype, timescale_pieces, time_pieces = filename.split('__')
        timescale = timescale_pieces.split('by_')[-1]
        from_time, to_time = time_pieces.split('_to_')
    except ValueError:
        raise Exception('path must be in format: ' + filename_template)
    return {
        'network': network,
        'datatype': datatype,
        'timescale': timescale,
        'from_time': from_time,
        'to_time': to_time,
    }


def parse_dir_files(dir_path: str) -> pl.DataFrame:
    return pl.DataFrame([parse_path(dir_path) for file in os.listdir(dir_path)])
