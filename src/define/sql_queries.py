"""
sql_queries.py
--------------

Centralized SQL queries for DuckDB operations in the Define PnL calculator.

All queries are defined as constants and imported by the query execution functions
in queries.py. This separation keeps business logic clean and makes SQL maintenance easier.
"""

# =====================================================================
# NFPM TRANSFER QUERIES
# =====================================================================

FETCH_TRANSFERS_QUERY = """
SELECT *
FROM read_parquet(?, union_by_name=true)
WHERE event = 'Transfer'
  AND (
    ("to" IN (?, ?))
    OR ("from" IN (?, ?))
  );
"""


# =====================================================================
# POOL EVENTS QUERIES
# =====================================================================

FETCH_GAUGE_DEPOSIT_WITHDRAW_QUERY = """
WITH raw AS (
  SELECT
    CAST(block_number AS BIGINT)                AS block_number,
    CAST(block_timestamp AS BIGINT)             AS ts,
    tx_hash,
    CAST(log_index AS BIGINT)                   AS log_index,
    contract,
    lower(event)                                AS ev,
    CAST(amount AS DOUBLE)                      AS amount,
    lower("from")                               AS owner,
    CAST(tokenId AS BIGINT)                     AS token_id
  FROM read_parquet(?, union_by_name=true)
)
SELECT
  token_id,
  ev AS kind,      -- 'collect' | 'deposit' | 'withdraw'
  block_number,
  ts,
  tx_hash,
  log_index,
  contract,
  owner,
  amount
FROM raw
JOIN token_ids USING (token_id)
ORDER BY token_id, block_number, tx_hash, log_index;
"""


# =====================================================================
# GAUGE COLLECT QUERIES
# =====================================================================

FETCH_GAUGE_COLLECTS_QUERY = """
SELECT
  CAST(block_number AS BIGINT)    AS block_number,
  CAST(block_timestamp AS BIGINT) AS ts,
  tx_hash,
  CAST(log_index AS BIGINT)       AS log_index,
  contract                        AS gauge_addr,
  CAST(amount AS DOUBLE)          AS amount,
  lower("from")                   AS from_addr
FROM read_parquet(?, union_by_name=true)
WHERE lower("from") IN (?, ?);
"""


# =====================================================================
# LIQUIDITY CHANGE CORRELATION QUERIES (MINT/BURN + INCREASE/DECREASE)
# =====================================================================

FETCH_LIQUIDITY_CORRELATION_QUERY = """
WITH nfpm AS (
  SELECT
    block_number,
    block_timestamp AS ts,
    tx_hash,
    log_index,
    contract,
    lower(event) AS ev,
    token_id,
    amount0,
    amount1,
    liquidity
  FROM read_parquet(?, union_by_name=true)
  WHERE lower(event) = ?
),
pool AS (
  SELECT
    block_number,
    block_timestamp AS ts,
    tx_hash,
    log_index,
    contract,
    lower(event) AS ev,
    tick_lower,
    tick_upper,
    amount0,
    amount1,
    liquidity
  FROM read_parquet(?, union_by_name=true)
  WHERE lower(event) = ?
)
SELECT
  nfpm.tx_hash,
  nfpm.token_id,
  nfpm.block_number AS nfpm_block,
  pool.block_number AS pool_block,
  nfpm.ts,
  nfpm.ev AS nfpm_event,
  pool.ev AS pool_event,
  nfpm.amount0 AS nfpm_amount0,
  nfpm.amount1 AS nfpm_amount1,
  nfpm.liquidity AS nfpm_liq,
  pool.amount0 AS pool_amount0,
  pool.amount1 AS pool_amount1,
  pool.liquidity AS pool_liq,
  pool.tick_lower,
  pool.tick_upper
FROM pool
JOIN nfpm USING (tx_hash)
ORDER BY nfpm.block_number, pool.log_index;
"""

