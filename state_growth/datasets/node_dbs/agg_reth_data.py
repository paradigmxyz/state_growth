from __future__ import annotations

import typing

import numpy as np
import polars as pl
import toolstr


def aggregate_by_entity(reth: pl.DataFrame) -> pl.DataFrame:
    return (
        reth.group_by("Scales with # of")
        .agg(pl.sum("total_bytes"), pl.max("# Entries"))
        .sort('total_bytes', descending=True)
        .filter(pl.col("Scales with # of") != "-")
        .with_columns(
            bytes_per_entity=pl.col.total_bytes / pl.col("# Entries"),
            total_bytes_str=pl.col.total_bytes.map_elements(
                lambda x: toolstr.format_nbytes(int(x))
            )
        )
    )


def compute_bytes_per_entity(reth: pl.DataFrame) -> typing.Mapping[str, float]:
    agg = aggregate_by_entity(reth)
    return dict(agg[["Scales with # of", "bytes_per_entity"]].rows())


# more precise aggregations, including logarithmic model, WIP

tables = {
    'accounts': {
        'linear': ['HashedAccount', 'PlainAccountState'],
        'log16': ['AccountsTrie'],
    },
    'slots': {
        'linear': ['HashedStorage', 'PlainStorageState'],
        'log16': ['StoragesTrie'],
    },
    'transactions': {
        'linear': ['Transactions', 'TxHashNumber', 'TxSenders'],
        'log16': [],
        # TransactionBlock as well, but it's small + idiosyncratic
    },
    'blocks': {
        'linear': [
            'BlockBodyIndices',
            'CanonicalHeaders',
            'HeaderNumbers',
            'HeaderTD',
            'Headers',
        ],
        'log16': [],
        # BlockOmmers + BlockWithdrawals as well, but they're small + idiosyncratic
    },
}


# def _compute_bytes_per_entity(reth: pl.DataFrame) -> None:
#     table_entries = dict(reth[['Table Name', '# Entries']].rows())
#     table_bytes = dict(reth[['Table Name', 'total_bytes']].rows())
#     table_categories = dict(reth[['Scales with # of', 'Table Category']].rows())
#     tables_by_entity = dict(
#         reth.group_by(pl.col("Scales with # of"))
#         .agg(pl.col("Table Name"))
#         .rows()
#     )

#     # accounts
#     n_accounts = table_entries[tables['accounts']['linear'][0]]
#     n_account_bytes = sum(
#         table_bytes[table_name]
#         for table_name in reth.filter(pl.col('Scales with # of') == 'Accounts')[
#             "Table Name"
#         ]
#     )
#     n_bytes_per_account = n_account_bytes / n_accounts

#     # slots
#     n_slots = table_entries[tables['slots']['linear'][0]]
#     n_slot_bytes = sum(
#         table_bytes[table_name]
#         for table_name in reth.filter(
#             pl.col('Scales with # of') == 'Storage slots'
#         )["Table Name"]
#     )
#     n_bytes_per_slot = n_slot_bytes / n_slots

#     # blocks
#     n_blocks = table_entries[tables['blocks']['linear'][0]]
#     n_block_bytes = sum(
#         table_bytes[table_name]
#         for table_name in reth.filter(pl.col('Scales with # of') == 'Blocks')[
#             "Table Name"
#         ]
#     )
#     n_bytes_per_block = n_block_bytes / n_blocks

#     # transactions
#     n_transactions = table_entries[tables['transactions']['linear'][0]]
#     n_transaction_bytes = sum(
#         table_bytes[table_name]
#         for table_name in reth.filter(
#             pl.col('Scales with # of') == 'Transactions'
#         )["Table Name"]
#     )
#     n_bytes_per_transaction = n_transaction_bytes / n_transactions

#     # logarithmic factors

#     constant_factor = table_entries['AccountsTrie'] / (
#         np.log(table_entries['PlainAccountState']) / np.log(16)
#     )

#     constant_factor * (np.log(table_entries['PlainAccountState']) / np.log(16))

#     # n_bytes_per_account = (
#     #     20  # address
#     #     + 32  # balance
#     #     + 32  # nonce
#     # )

#     n_bytes_per_contract = (
#         0
#         #     20  # address
#     )

#     # n_bytes_per_slot = (
#     #     20  # slot
#     #     + 32  # value
#     # )

#     print('n_bytes_per_account:     ', n_bytes_per_account)
#     print('n_bytes_per_contract:    ', n_bytes_per_contract)
#     print('n_bytes_per_slot:        ', n_bytes_per_slot)
#     print('n_bytes_per_block:       ', n_bytes_per_block)
#     print('n_bytes_per_transaction: ', n_bytes_per_transaction)

