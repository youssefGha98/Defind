"""
queries.py
----------

Centralized module for all DuckDB query execution functions used in Define.

Includes:
    - NFPM Transfer events (to identify positions owned by given wallets)
    - Pool-level events (deposit / withdraw / collect)
    - Gauge collects (reward claims)
    - Mint / IncreaseLiquidity correlations between NFPM and CLPool

Each function initializes its own DuckDB connection with performance PRAGMAs
and properly cleans up resources after execution.
All queries return clean pandas DataFrames ready for downstream analysis.
"""

import duckdb
import pandas as pd
from typing import List
from contextlib import contextmanager

from . import sql_queries


# =====================================================================
# DuckDB connection setup
# =====================================================================

@contextmanager
def get_connection(memory_limit: str = "8GB", threads: int = 8):
    """Context manager for DuckDB connections with optimized PRAGMAs.

    Args:
        memory_limit: Maximum memory allocation for DuckDB.
        threads: Number of threads for parallel execution.

    Yields:
        DuckDB connection with configured performance settings.
    """
    con = duckdb.connect()
    try:
        con.execute(f"PRAGMA threads={threads}")
        con.execute(f"PRAGMA memory_limit='{memory_limit}'")
        con.execute("PRAGMA enable_object_cache=true")
        yield con
    finally:
        con.close()


# =====================================================================
# NFPM TRANSFERS
# =====================================================================

def fetch_transfers(path: str, wallet: str, vfat: str) -> pd.DataFrame:
    """Fetch all Transfer events from the NFPM contract
    involving either the user wallet or the VFAT helper.

    Args:
        path: Parquet glob path for NFPM Transfer events.
        wallet: User wallet address (case-insensitive).
        vfat: VFAT wallet address (case-insensitive).

    Returns:
        DataFrame with all matching Transfer events.
    """
    with get_connection() as con:
        params = [path, vfat.lower(), wallet.lower(), vfat.lower(), wallet.lower()]
        return con.execute(sql_queries.FETCH_TRANSFERS_QUERY, params).df()


# =====================================================================
# POOL EVENTS (deposit / withdraw / collect)
# =====================================================================

def fetch_gauge_deposit_withdraw_events(path: str, token_ids: List[int]) -> pd.DataFrame:
    """Fetch deposit / withdraw / collect events for a set of NFPM token_ids.

    Args:
        path: Parquet glob path for the CLGauge or NFPM event dataset.
        token_ids: List of token_id integers.

    Returns:
        DataFrame with all matched events.
    """
    with get_connection() as con:
        # Register temporary table with token IDs
        con.register("token_ids", pd.DataFrame({"token_id": token_ids}))
        try:
            return con.execute(sql_queries.FETCH_GAUGE_DEPOSIT_WITHDRAW_QUERY, [path]).df()
        finally:
            con.unregister("token_ids")


# =====================================================================
# GAUGE COLLECTS
# =====================================================================

def fetch_gauge_collects(path: str, wallet: str, vfat: str) -> pd.DataFrame:
    """Fetch Collect events emitted by gauges for a given wallet or VFAT proxy.

    Args:
        path: Parquet glob path for gauge events.
        wallet: User wallet address (case-insensitive).
        vfat: VFAT proxy address (case-insensitive).

    Returns:
        DataFrame of collect events.
    """
    with get_connection() as con:
        params = [path, wallet.lower(), vfat.lower()]
        return con.execute(sql_queries.FETCH_GAUGE_COLLECTS_QUERY, params).df()


# =====================================================================
# LIQUIDITY CORRELATION (MINT/BURN + INCREASE/DECREASE)
# =====================================================================

def fetch_liquidity_correlation(
    nfpm_path: str, 
    pool_path: str, 
    nfpm_event: str, 
    pool_event: str
) -> pd.DataFrame:
    """Join NFPM liquidity events with corresponding CLPool events.

    Args:
        nfpm_path: Parquet glob path to NFPM events.
        pool_path: Parquet glob path to CLPool events.
        nfpm_event: NFPM event type ('increaseliquidity' or 'decreaseliquidity').
        pool_event: Pool event type ('mint' or 'burn').

    Returns:
        DataFrame mapping liquidity changes with tick ranges and amounts.
    """
    with get_connection() as con:
        params = [nfpm_path, nfpm_event.lower(), pool_path, pool_event.lower()]
        return con.execute(sql_queries.FETCH_LIQUIDITY_CORRELATION_QUERY, params).df()


def fetch_mint_increase(nfpm_path: str, pool_path: str) -> pd.DataFrame:
    """Join NFPM IncreaseLiquidity events with corresponding CLPool Mint events.

    Args:
        nfpm_path: Parquet glob path to NFPM IncreaseLiquidity events.
        pool_path: Parquet glob path to CLPool Mint events.

    Returns:
        DataFrame mapping liquidity additions with tick ranges and amounts.
    """
    return fetch_liquidity_correlation(nfpm_path, pool_path, 'increaseliquidity', 'mint')


def fetch_burn_decrease(nfpm_path: str, pool_path: str) -> pd.DataFrame:
    """Join NFPM DecreaseLiquidity events with corresponding CLPool Burn events.

    Args:
        nfpm_path: Parquet glob path to NFPM DecreaseLiquidity events.
        pool_path: Parquet glob path to CLPool Burn events.

    Returns:
        DataFrame mapping liquidity removals with tick ranges and amounts.
    """
    return fetch_liquidity_correlation(nfpm_path, pool_path, 'decreaseliquidity', 'burn')