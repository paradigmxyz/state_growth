from __future__ import annotations

import datetime
import tooltime

import state_growth


def timestamp_to_str(timestamp: tooltime.Timestamp) -> str:
    return tooltime.timestamp_to_datetime(timestamp).strftime(
        state_growth.agg_time_format
    )


def str_to_timestamp(raw_str: str) -> int:
    dt = datetime.datetime.strptime(raw_str, state_growth.agg_time_format)
    return tooltime.timestamp_to_seconds(dt)
