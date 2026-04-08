"""
Liquidity Updater Service
=========================
Runs as a standalone background worker (see Procfile: liquidity-updater).

Every UPDATE_INTERVAL seconds (default 300 = 5 minutes) it:
  1. Fetches all pair addresses from abandoned_tokens.
  2. Queries DexScreener for the current USD liquidity of each pair.
  3. Appends a row to liquidity_history.
  4. Updates liquidity_usd on the abandoned_tokens row.

Designed to be resilient: individual pair failures are logged and skipped;
the loop continues regardless.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

import aiohttp
import asyncpg

from database import (
    close_pool,
    ensure_tables,
    fetch_all_pairs,
    get_pool,
    insert_liquidity_snapshot,
    update_token_liquidity,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("liquidity_updater")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
UPDATE_INTERVAL: int = int(os.environ.get("LIQUIDITY_UPDATE_INTERVAL", "300"))  # seconds
DEXSCREENER_URL = "https://api.dexscreener.com/latest/dex/pairs/bsc/{address}"
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)
CONCURRENCY = 5          # max simultaneous DexScreener requests
RATE_LIMIT_DELAY = 0.3   # seconds between batches


# ---------------------------------------------------------------------------
# DexScreener fetch (async)
# ---------------------------------------------------------------------------

async def fetch_liquidity(session: aiohttp.ClientSession, pair_address: str) -> float:
    """
    Query DexScreener for the current USD liquidity of `pair_address`.
    Returns 0.0 on any error.
    """
    url = DEXSCREENER_URL.format(address=pair_address.lower())
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
            pairs = data.get("pairs") or []
            if not pairs:
                log.debug("No DexScreener data for pair %s", pair_address)
                return 0.0
            liquidity = pairs[0].get("liquidity") or {}
            value = float(liquidity.get("usd") or 0.0)
            log.debug("Pair %s  liquidity_usd=%.2f", pair_address, value)
            return value
    except asyncio.TimeoutError:
        log.warning("DexScreener timeout for pair %s", pair_address)
        return 0.0
    except aiohttp.ClientResponseError as exc:
        log.warning("DexScreener HTTP %s for pair %s", exc.status, pair_address)
        return 0.0
    except Exception as exc:
        log.warning("DexScreener error for pair %s: %s", pair_address, exc)
        return 0.0


# ---------------------------------------------------------------------------
# Core update logic
# ---------------------------------------------------------------------------

async def update_all_pairs(pool: asyncpg.Pool) -> None:
    """
    Fetch liquidity for every tracked pair and persist the results.
    Uses a semaphore to cap concurrent HTTP requests.
    """
    async with pool.acquire() as conn:
        pairs = await fetch_all_pairs(conn)

    if not pairs:
        log.info("No pairs in abandoned_tokens – nothing to update")
        return

    log.info("Updating liquidity for %d pairs...", len(pairs))
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    sem = asyncio.Semaphore(CONCURRENCY)

    async def _update_one(session: aiohttp.ClientSession, pair: str) -> None:
        async with sem:
            liquidity = await fetch_liquidity(session, pair)
            try:
                async with pool.acquire() as conn:
                    await insert_liquidity_snapshot(conn, pair, liquidity, now)
                    await update_token_liquidity(conn, pair, liquidity)
                log.info("Updated pair %s  liquidity_usd=%.2f", pair, liquidity)
            except Exception as exc:
                log.error("DB write failed for pair %s: %s", pair, exc)
            # Polite rate-limiting between individual requests
            await asyncio.sleep(RATE_LIMIT_DELAY)

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [asyncio.create_task(_update_one(session, p)) for p in pairs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    errors = sum(1 for r in results if isinstance(r, Exception))
    log.info(
        "Liquidity update complete: %d pairs processed, %d errors",
        len(pairs),
        errors,
    )


# ---------------------------------------------------------------------------
# Startup: ensure DB is ready
# ---------------------------------------------------------------------------

async def _init_db(pool: asyncpg.Pool) -> None:
    """Create tables if they don't exist yet."""
    async with pool.acquire() as conn:
        await ensure_tables(conn)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def run() -> None:
    log.info("=== Liquidity Updater starting (interval=%ds) ===", UPDATE_INTERVAL)

    pool = await get_pool()
    await _init_db(pool)

    cycle = 0
    while True:
        cycle += 1
        log.info("--- Liquidity update cycle #%d ---", cycle)
        try:
            await update_all_pairs(pool)
        except Exception as exc:
            log.error("Unhandled error in cycle #%d: %s", cycle, exc, exc_info=True)

        log.info("Sleeping %d seconds until next update...", UPDATE_INTERVAL)
        await asyncio.sleep(UPDATE_INTERVAL)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Liquidity Updater stopped by user")
    finally:
        asyncio.run(close_pool())
