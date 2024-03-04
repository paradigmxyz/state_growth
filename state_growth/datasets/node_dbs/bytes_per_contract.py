from __future__ import annotations

import polars as pl

from . import load_reth_data


def compute_bytes_per_contract(
    contracts: pl.DataFrame, reth_db_path: str,
) -> None:
    bytes_per_code_hash = compute_bytes_per_code_hash(contracts=contracts)
    bytecode_inflation_factor = get_bytecode_inflation_factor(
        reth_db_path=reth_db_path,
        bytes_per_code_hash=bytes_per_code_hash,
    )

    return (
        contracts[["contract_address", "code_hash"]]
        .join(
            bytes_per_code_hash[["code_hash", "bytes_per_copy"]],
            on="code_hash",
            how="left",
        )
        .rename({"bytes_per_copy": "raw_bytes_per_copy"})
        .with_columns(
            n_stored_bytes=bytecode_inflation_factor * pl.col.raw_bytes_per_copy
        )
    )


def compute_bytes_per_code_hash(contracts: pl.DataFrame) -> pl.DataFrame:
    return (
        contracts.group_by("code_hash")
        .agg(
            n_bytes=pl.first("n_code_bytes"),
            n_contracts=pl.len(),
        )
        .with_columns(
            bytes_per_copy=pl.col.n_bytes / pl.col.n_contracts,
        )
    )


def get_bytecode_inflation_factor(
    reth_db_path: str,
    bytes_per_code_hash: pl.DataFrame,
) -> pl.DataFrame:
    bytecodes_table = load_reth_data.load_reth_db_table('Bytecodes', reth_db_path)
    size_on_disk = bytecodes_table['total_bytes']
    return size_on_disk / bytes_per_code_hash["n_bytes"].sum()
