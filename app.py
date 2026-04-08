"""
FastAPI dashboard backend for the Zombie Scanner.

Routes:
  GET /                          → serves static/index.html
  GET /health                    → health check
  GET /tokens                    → all rows from abandoned_tokens
  GET /tokens/with-liquidity     → tokens joined with latest liquidity snapshot
  GET /liquidity                 → current liquidity for all tracked pairs
  GET /liquidity/stats           → aggregate liquidity statistics per pair
  GET /liquidity/{pair_address}  → full liquidity history for one pair
"""

import os
from pathlib import Path

import asyncpg
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from database import (
    fetch_all_current_liquidity,
    fetch_liquidity_history,
    fetch_liquidity_stats,
    get_pool,
    normalize_dsn,
)

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(title="Zombie Scanner Dashboard", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "")

STATIC_DIR = Path(__file__).parent / "static"
INDEX_HTML = STATIC_DIR / "index.html"


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

async def _get_connection() -> asyncpg.Connection:
    """Open a single-use connection (used by legacy /tokens endpoint)."""
    if not DATABASE_URL:
        raise HTTPException(status_code=503, detail="DATABASE_URL is not configured")
    try:
        return await asyncpg.connect(normalize_dsn(DATABASE_URL), timeout=10)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")


async def _pool_conn():
    """Acquire a connection from the shared pool."""
    try:
        pool = await get_pool()
        return pool
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database pool unavailable: {exc}")


# ---------------------------------------------------------------------------
# Routes – health & root
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    """Simple liveness probe."""
    return {"status": "ok", "version": "2.0.0"}


@app.get("/")
async def root():
    """Serve the dashboard HTML."""
    if not INDEX_HTML.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(str(INDEX_HTML), media_type="text/html")


# ---------------------------------------------------------------------------
# Routes – tokens
# ---------------------------------------------------------------------------

@app.get("/tokens")
async def get_tokens():
    """Return all rows from abandoned_tokens ordered by detection time."""
    conn = await _get_connection()
    try:
        rows = await conn.fetch(
            """
            SELECT
                id,
                pair_address,
                token0,
                token1,
                CAST(liquidity_usd AS FLOAT) AS liquidity_usd,
                detected_at
            FROM abandoned_tokens
            ORDER BY detected_at DESC
            """
        )
        result = []
        for r in rows:
            row = dict(r)
            if row.get("detected_at"):
                row["detected_at"] = row["detected_at"].isoformat()
            result.append(row)
        return JSONResponse(content=result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        await conn.close()


@app.get("/tokens/with-liquidity")
async def get_tokens_with_liquidity():
    """
    Return all tracked tokens joined with their most-recent liquidity snapshot
    from liquidity_history.  Falls back to the value stored in abandoned_tokens
    when no history row exists yet.
    """
    try:
        pool = await _pool_conn()
        async with pool.acquire() as conn:
            data = await fetch_all_current_liquidity(conn)
        return JSONResponse(content=data)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Routes – liquidity
# ---------------------------------------------------------------------------

@app.get("/liquidity")
async def get_liquidity():
    """
    Return the current (most-recent snapshot) liquidity for every tracked pair.
    Identical to /tokens/with-liquidity but semantically scoped to liquidity data.
    """
    try:
        pool = await _pool_conn()
        async with pool.acquire() as conn:
            data = await fetch_all_current_liquidity(conn)
        return JSONResponse(content=data)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/liquidity/stats")
async def get_liquidity_stats_all():
    """
    Return aggregate statistics (min, max, avg, trend %) for every tracked pair
    that has at least one liquidity_history row.
    """
    try:
        pool = await _pool_conn()
        async with pool.acquire() as conn:
            # Get all distinct pairs that have history
            rows = await conn.fetch(
                """
                SELECT DISTINCT pair_address
                FROM liquidity_history
                ORDER BY pair_address
                """
            )
            pairs = [r["pair_address"] for r in rows]
            stats_list = []
            for pair in pairs:
                s = await fetch_liquidity_stats(conn, pair)
                stats_list.append(s)
        return JSONResponse(content=stats_list)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/liquidity/{pair_address}")
async def get_liquidity_history(
    pair_address: str,
    limit: int = Query(default=288, ge=1, le=2000,
                       description="Max data points to return (default 288 = 24 h at 5-min intervals)"),
):
    """
    Return the liquidity history for a single pair, ordered oldest → newest.
    Suitable for feeding directly into a time-series chart.
    """
    try:
        pool = await _pool_conn()
        async with pool.acquire() as conn:
            history = await fetch_liquidity_history(conn, pair_address, limit=limit)
        if not history:
            raise HTTPException(
                status_code=404,
                detail=f"No liquidity history found for pair {pair_address}",
            )
        return JSONResponse(content=history)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
