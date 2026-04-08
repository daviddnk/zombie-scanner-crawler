"""
FastAPI dashboard backend for the Zombie Scanner.

Routes:
  GET /         → serves static/index.html
  GET /tokens   → JSON list of all rows from abandoned_tokens
  GET /health   → health check
"""

import os
from pathlib import Path

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(title="Zombie Scanner Dashboard", version="1.0.0")

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


def _normalize_dsn(url: str) -> str:
    """asyncpg requires postgresql:// not postgres://"""
    if url and url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
async def _get_connection() -> asyncpg.Connection:
    if not DATABASE_URL:
        raise HTTPException(status_code=503, detail="DATABASE_URL is not configured")
    try:
        return await asyncpg.connect(_normalize_dsn(DATABASE_URL), timeout=10)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    """Simple liveness probe."""
    return {"status": "ok"}


@app.get("/tokens")
async def get_tokens():
    """Return all rows from abandoned_tokens as JSON."""
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
            # Convert datetime to ISO string for JSON serialisation
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


@app.get("/")
async def root():
    """Serve the dashboard HTML."""
    if not INDEX_HTML.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(str(INDEX_HTML), media_type="text/html")
