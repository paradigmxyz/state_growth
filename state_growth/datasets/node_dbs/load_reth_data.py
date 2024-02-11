from __future__ import annotations

import polars as pl


def load_reth_db_sizes(path: str) -> pl.DataFrame:
    reth = pl.read_csv(path)

    reth = reth.with_columns(
        pl.col('# Entries').map_elements(lambda x: float(x.replace(',', '')))
    )

    total_bytes = (
        reth.with_columns(
            byte_number=pl.col.Size.str.split(' ').list.get(0),
            byte_suffix=pl.col.Size.str.split(' ').list.get(1),
        )
        .with_columns(
            multiplier=pl.when(pl.col.byte_suffix == 'B')
            .then(1)
            .when(pl.col.byte_suffix == 'KiB')
            .then(1024)
            .when(pl.col.byte_suffix == 'MiB')
            .then(1024**2)
            .when(pl.col.byte_suffix == 'GiB')
            .then(1024**3)
            .when(pl.col.byte_suffix == 'TiB')
            .then(1024**4)
            .otherwise(-9999999999999),
            byte_number=pl.col.byte_number.cast(pl.Float64),
        )
        .select(total_bytes=pl.col.byte_number * pl.col.multiplier)
    )

    reth = reth.with_columns(total_bytes)

    reth = reth.with_columns(
        pl.when(pl.col('Scales with # of') == pl.lit('TxSenders'))
        .then(pl.lit('Transactions'))
        .otherwise(pl.col('Scales with # of'))
        .alias('Scales with # of')
    )

    return reth
