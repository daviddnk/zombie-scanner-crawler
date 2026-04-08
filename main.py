import asyncio
import logging
import os
from datetime import datetime, timezone

import asyncpg

from crawler.scanner import PancakeSwapScanner

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")
SCAN_INTERVAL = 30  # seconds between scans

# asyncpg requires the postgresql:// scheme (not postgres://)
def _normalize_dsn(url: str) -> str:
    if url and url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
async def connect_db(retries: int = 10, delay: int = 5) -> asyncpg.Connection:
    """Connect to PostgreSQL with retries."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    dsn = _normalize_dsn(DATABASE_URL)
    for attempt in range(1, retries + 1):
        try:
            conn = await asyncpg.connect(dsn, timeout=10)
            log.info("Connected to PostgreSQL (attempt %d)", attempt)
            return conn
        except Exception as exc:
            log.warning(
                "DB connection failed (attempt %d/%d): %s", attempt, retries, exc
            )
            if attempt < retries:
                await asyncio.sleep(delay)

    raise RuntimeError(f"Could not connect to PostgreSQL after {retries} attempts")


async def create_table(conn: asyncpg.Connection) -> None:
    """Create abandoned_tokens table if it does not exist."""
    ddl = """
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
    await conn.execute(ddl)
    log.info("Table abandoned_tokens is ready")


async def save_tokens(conn: asyncpg.Connection, tokens: list) -> int:
    """Persist a list of abandoned-token dicts. Returns number of rows inserted."""
    if not tokens:
        return 0

    insert = """
        INSERT INTO abandoned_tokens
            (pair_address, token0, token1, liquidity_usd, detected_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT DO NOTHING;
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [
        (
            t["pair"],
            t.get("token0"),
            t.get("token1"),
            t.get("liquidity_usd"),
            t.get("timestamp", now),
        )
        for t in tokens
    ]

    await conn.executemany(insert, rows)
    log.info("Saved %d token rows to database", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------
class ZombieScannerWorker:
    def __init__(self):
        self.conn: asyncpg.Connection | None = None
        self.scanner = PancakeSwapScanner()

    async def _ensure_connection(self) -> None:
        """Re-connect if the DB connection was lost or never opened."""
        if self.conn is not None:
            try:
                await self.conn.fetchval("SELECT 1")
                return  # connection is alive
            except Exception:
                log.warning("DB connection lost – reconnecting...")
                try:
                    await self.conn.close()
                except Exception:
                    pass
                self.conn = None

        log.info("(Re)connecting to database...")
        self.conn = await connect_db()
        await create_table(self.conn)

    async def run(self) -> None:
        log.info("=== Zombie Scanner Worker starting ===")
        await self._ensure_connection()

        cycle = 0
        while True:
            cycle += 1
            log.info("--- Scan cycle #%d ---", cycle)

            try:
                await self._ensure_connection()

                # scanner is synchronous (Web3 + requests); run in thread pool
                loop = asyncio.get_running_loop()
                abandoned = await loop.run_in_executor(
                    None, self.scanner.scan_latest_pairs
                )
                log.info("Abandoned tokens found this cycle: %d", len(abandoned))

                if abandoned:
                    saved = await save_tokens(self.conn, abandoned)
                    log.info("Rows inserted into DB: %d", saved)
                else:
                    log.info("No abandoned tokens detected this cycle")

            except Exception as exc:
                log.error(
                    "Error during scan cycle #%d: %s", cycle, exc, exc_info=True
                )

            log.info("Sleeping %d seconds until next cycle...", SCAN_INTERVAL)
            await asyncio.sleep(SCAN_INTERVAL)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(ZombieScannerWorker().run())

