"""
Shared database utilities for the Zombie Scanner.

Responsibilities:
  - Connection pooling helpers (asyncpg Pool)
  - DDL: create abandoned_tokens and liquidity_history tables
  - DML: insert/update liquidity data, fetch liquidity history
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

import asyncpg

log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")


# ---------------------------------------------------------------------------
# DSN normalisation
# ---------------------------------------------------------------------------

def normalize_dsn(url: str) -> str:
    """asyncpg requires postgresql:// not postgres://"""
    if url and url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


# ---------------------------------------------------------------------------
# Pool management
# ---------------------------------------------------------------------------

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    """Return (or lazily create) the shared connection pool."""
    global _pool
    if _pool is None or _pool._closed:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL environment variable is not set")
        _pool = await asyncpg.create_pool(
            normalize_dsn(DATABASE_URL),
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
        log.info("asyncpg connection pool created (min=2, max=10)")
    return _pool


async def close_pool() -> None:
    """Gracefully close the shared pool."""
    global _pool
    if _pool and not _pool._closed:
        await _pool.close()
        log.info("asyncpg connection pool closed")
    _pool = None


# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

DDL_ABANDONED_TOKENS = """
    CREATE TABLE IF NOT EXISTS abandoned_tokens (
        id            SERIAL PRIMARY KEY,
        pair_address  VARCHAR(42)  NOT NULL,
        token0        VARCHAR(42),
        token1        VARCHAR(42),
        liquidity_usd NUMERIC(20, 4),
        detected_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
        UNIQUE (pair_address, detected_at)
    );
"""

DDL_LIQUIDITY_HISTORY = """
    CREATE TABLE IF NOT EXISTS liquidity_history (
        id            SERIAL PRIMARY KEY,
        pair_address  VARCHAR(42)  NOT NULL,
        liquidity_usd NUMERIC(20, 4),
        recorded_at   TIMESTAMP    NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_lh_pair_address
        ON liquidity_history (pair_address);
    CREATE INDEX IF NOT EXISTS idx_lh_recorded_at
        ON liquidity_history (recorded_at DESC);
"""


async def ensure_tables(conn: asyncpg.Connection) -> None:
    """Create all required tables and indexes if they do not already exist."""
    await conn.execute(DDL_ABANDONED_TOKENS)
    log.info("Table abandoned_tokens is ready")
    await conn.execute(DDL_LIQUIDITY_HISTORY)
    log.info("Table liquidity_history is ready")


# ---------------------------------------------------------------------------
# Liquidity history: write
# ---------------------------------------------------------------------------

async def insert_liquidity_snapshot(
    conn: asyncpg.Connection,
    pair_address: str,
    liquidity_usd: float,
    recorded_at: Optional[datetime] = None,
) -> None:
    """Append one liquidity data point to liquidity_history."""
    if recorded_at is None:
        recorded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await conn.execute(
        """
        INSERT INTO liquidity_history (pair_address, liquidity_usd, recorded_at)
        VALUES ($1, $2, $3)
        """,
        pair_address.lower(),
        liquidity_usd,
        recorded_at,
    )


async def update_token_liquidity(
    conn: asyncpg.Connection,
    pair_address: str,
    liquidity_usd: float,
) -> None:
    """Update the current liquidity_usd on the most-recent abandoned_tokens row."""
    await conn.execute(
        """
        UPDATE abandoned_tokens
        SET    liquidity_usd = $2
        WHERE  pair_address  = $1
        """,
        pair_address.lower(),
        liquidity_usd,
    )


# ---------------------------------------------------------------------------
# Liquidity history: read
# ---------------------------------------------------------------------------

async def fetch_all_current_liquidity(conn: asyncpg.Connection) -> list[dict]:
    """
    Return the latest liquidity snapshot for every tracked pair.
    Joins abandoned_tokens with the most-recent liquidity_history row.
    """
    rows = await conn.fetch(
        """
        SELECT
            at.pair_address,
            at.token0,
            at.token1,
            CAST(at.liquidity_usd AS FLOAT)  AS liquidity_usd,
            at.detected_at,
            lh.recorded_at                   AS last_updated
        FROM abandoned_tokens at
        LEFT JOIN LATERAL (
            SELECT liquidity_usd, recorded_at
            FROM   liquidity_history
            WHERE  pair_address = at.pair_address
            ORDER  BY recorded_at DESC
            LIMIT  1
        ) lh ON TRUE
        ORDER BY at.detected_at DESC
        """
    )
    result = []
    for r in rows:
        row = dict(r)
        if row.get("detected_at"):
            row["detected_at"] = row["detected_at"].isoformat()
        if row.get("last_updated"):
            row["last_updated"] = row["last_updated"].isoformat()
        result.append(row)
    return result


async def fetch_liquidity_history(
    conn: asyncpg.Connection,
    pair_address: str,
    limit: int = 288,          # 288 × 5 min = 24 h
) -> list[dict]:
    """
    Return up to `limit` historical liquidity snapshots for a single pair,
    ordered oldest-first (suitable for charting).
    """
    rows = await conn.fetch(
        """
        SELECT
            id,
            pair_address,
            CAST(liquidity_usd AS FLOAT) AS liquidity_usd,
            recorded_at
        FROM liquidity_history
        WHERE pair_address = $1
        ORDER BY recorded_at ASC
        LIMIT $2
        """,
        pair_address.lower(),
        limit,
    )
    result = []
    for r in rows:
        row = dict(r)
        if row.get("recorded_at"):
            row["recorded_at"] = row["recorded_at"].isoformat()
        result.append(row)
    return result


async def fetch_liquidity_stats(
    conn: asyncpg.Connection,
    pair_address: str,
) -> dict:
    """
    Return min / max / avg liquidity and a simple trend indicator
    (positive = rising, negative = falling, zero = flat) for a pair.
    """
    row = await conn.fetchrow(
        """
        SELECT
            CAST(MIN(liquidity_usd) AS FLOAT) AS min_liquidity,
            CAST(MAX(liquidity_usd) AS FLOAT) AS max_liquidity,
            CAST(AVG(liquidity_usd) AS FLOAT) AS avg_liquidity,
            COUNT(*)                          AS data_points
        FROM liquidity_history
        WHERE pair_address = $1
        """,
        pair_address.lower(),
    )

    stats = dict(row) if row else {}

    # Trend: compare the average of the first 10 % of points vs the last 10 %
    trend = 0.0
    count = stats.get("data_points") or 0
    if count >= 4:
        window = max(1, count // 10)
        early = await conn.fetchval(
            """
            SELECT AVG(liquidity_usd)
            FROM (
                SELECT liquidity_usd
                FROM   liquidity_history
                WHERE  pair_address = $1
                ORDER  BY recorded_at ASC
                LIMIT  $2
            ) sub
            """,
            pair_address.lower(),
            window,
        )
        recent = await conn.fetchval(
            """
            SELECT AVG(liquidity_usd)
            FROM (
                SELECT liquidity_usd
                FROM   liquidity_history
                WHERE  pair_address = $1
                ORDER  BY recorded_at DESC
                LIMIT  $2
            ) sub
            """,
            pair_address.lower(),
            window,
        )
        if early and recent and float(early) > 0:
            trend = round((float(recent) - float(early)) / float(early) * 100, 2)

    stats["trend_pct"] = trend
    stats["pair_address"] = pair_address.lower()
    return stats


async def fetch_all_pairs(conn: asyncpg.Connection) -> list[str]:
    """Return all distinct pair addresses currently in abandoned_tokens."""
    rows = await conn.fetch(
        "SELECT DISTINCT pair_address FROM abandoned_tokens ORDER BY pair_address"
    )
    return [r["pair_address"] for r in rows]
