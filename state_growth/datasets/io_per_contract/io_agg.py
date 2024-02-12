from __future__ import annotations

import polars as pl

import state_growth


def aggregate_contract_slot_reads(df: pl.DataFrame, time_column: str) -> pl.DataFrame:
    return (
        df.group_by(time_column, 'contract_address')
        .agg(
            n_reads=pl.len(),
            n_unique_read_slots=pl.n_unique('slot'),
        )
        .sort(time_column, 'n_reads', descending=[False, True])
    )


def aggregate_contract_slot_diffs(df: pl.DataFrame, time_column: str) -> pl.DataFrame:
    creates = df.filter(pl.col.from_value == state_growth.binary_zero_word)
    updates = df.filter(
        (pl.col.from_value != state_growth.binary_zero_word)
        & (pl.col.to_value != state_growth.binary_zero_word)
    )
    deletes = df.filter(pl.col.to_value == state_growth.binary_zero_word)

    write_agg = df.group_by(time_column, 'address').agg(
        n_slot_writes=pl.len(), n_unique_written_slots=pl.n_unique('slot')
    )
    create_agg = creates.group_by(time_column, 'address').agg(
        n_slot_creates=pl.len(), n_unique_created_slots=pl.n_unique('slot')
    )
    update_agg = updates.group_by(time_column, 'address').agg(
        n_slot_updates=pl.len(), n_unique_updated_slots=pl.n_unique('slot')
    )
    delete_agg = deletes.group_by(time_column, 'address').agg(
        n_slot_deletes=pl.len(), n_unique_deleted_slots=pl.n_unique('slot')
    )

    return (
        write_agg.join(create_agg, on=[time_column, 'address'], how='left')
        .drop(time_column + '_right')
        .join(update_agg, on=[time_column, 'address'], how='left')
        .drop(time_column + '_right')
        .join(delete_agg, on=[time_column, 'address'], how='left')
        .drop(time_column + '_right')
        .fill_null(0)
        .sort(time_column)
    )


def compute_contract_slot_agg_proportions(agg: pl.DataFrame) -> pl.DataFrame:
    exprs = dict(
        prop_slot_writes=pl.sum('n_slot_writes') / agg['n_slot_writes'].sum(),
        prop_slot_creates=pl.sum('n_slot_creates') / agg['n_slot_creates'].sum(),
        prop_slot_updates=pl.sum('n_slot_updates') / agg['n_slot_updates'].sum(),
        prop_slot_deletes=pl.sum('n_slot_deletes') / agg['n_slot_deletes'].sum(),
    )

    if 'n_unique_written_slots' in agg.columns:
        exprs['prop_unique_slot_writes'] = (
            pl.sum('n_unique_written_slots') / agg['n_unique_written_slots'].sum()
        )
    if 'n_unique_created_slots' in agg.columns:
        exprs['prop_unique_slot_creates'] = (
            pl.sum('n_unique_created_slots') / agg['n_unique_created_slots'].sum()
        )
    if 'n_unique_updated_slots' in agg.columns:
        exprs['prop_unique_slot_updates'] = (
            pl.sum('n_unique_updated_slots') / agg['n_unique_updated_slots'].sum()
        )
    if 'n_unique_deleted_slots' in agg.columns:
        exprs['prop_unique_slot_deletes'] = (
            pl.sum('n_unique_deleted_slots') / agg['n_unique_deleted_slots'].sum()
        )

    return (
        agg.group_by('address').agg(**exprs).sort('prop_slot_writes', descending=True)
    )
