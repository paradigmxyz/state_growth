from __future__ import annotations

import typing

import polars as pl


def join_contracts(df: pl.DataFrame, contracts: pl.DataFrame) -> pl.DataFrame:
    return df.join(
        contracts,
        left_on="address",
        right_on="contract_address",
        how="left",
    )


def agg_by_contract_field(
    agg: pl.DataFrame, contracts: pl.DataFrame, field: str
) -> pl.DataFrame:
    return (
        join_contracts(agg, contracts[['contract_address', field]])
        .group_by(field)
        .agg(
            n_contracts=pl.len(),
            n_slot_writes=pl.sum('n_slot_writes'),
            n_slot_creates=pl.sum('n_slot_creates'),
            n_slot_updates=pl.sum('n_slot_updates'),
            n_slot_deletes=pl.sum('n_slot_deletes'),
            # pl.sum('n_unique_written_slots'),
            # pl.sum('n_unique_created_slots'),
            # pl.sum('n_unique_updated_slots'),
            # pl.sum('n_unique_deleted_slots'),
        )
        .with_columns(
            prop_of_n_writes=pl.col.n_slot_writes / pl.sum('n_slot_writes'),
            prop_of_n_creates=pl.col.n_slot_creates / pl.sum('n_slot_creates'),
            prop_of_n_updates=pl.col.n_slot_updates / pl.sum('n_slot_updates'),
            prop_of_n_deletes=pl.col.n_slot_deletes / pl.sum('n_slot_deletes'),
        )
    )


def agg_by_contract_fields(
    agg: pl.DataFrame, contracts: pl.DataFrame
) -> typing.Mapping[str, pl.DataFrame]:
    agg_by_init_code = agg_by_contract_field(
        agg, contracts, 'init_code_hash'
    ).sort('prop_of_n_creates', descending=True)
    agg_by_factory = agg_by_contract_field(agg, contracts, 'factory').sort(
        'prop_of_n_creates', descending=True
    )
    agg_by_deployer = agg_by_contract_field(agg, contracts, 'deployer').sort(
        'prop_of_n_creates', descending=True
    )

    return {
        'agg_by_init_code': agg_by_init_code,
        'agg_by_factory': agg_by_factory,
        'agg_by_deployer': agg_by_deployer,
    }


def plot_cumulative_creates_by_contract_fields(
    aggs: typing.Mapping[str, pl.DataFrame], n: int = 10000
) -> None:
    import matplotlib.pyplot as plt
    import toolplot

    plt.plot(
        aggs['agg_by_init_code']['prop_of_n_creates'][:n].cum_sum(),
        label='by top init code',
    )
    plt.plot(
        aggs['agg_by_factory']['prop_of_n_creates'][:n].cum_sum(),
        label='by top factory',
    )
    plt.plot(
        aggs['agg_by_deployer']['prop_of_n_creates'][:n].cum_sum(),
        label='by top deployer',
    )
    plt.xscale('log')
    toolplot.format_yticks(toolstr_kwargs={'percentage': True})
    toolplot.format_xticks()
    plt.xlabel('top n')
    plt.ylabel('% of creates')
    plt.legend(loc='lower right')
    toolplot.add_tick_grid()
    plt.title('Cumulative distribution of creates\nby contract fields')

