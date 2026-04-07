import os
import time
import logging
import psycopg2
from psycopg2 import OperationalError
from datetime import datetime

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


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def connect_db(retries: int = 10, delay: int = 5) -> psycopg2.extensions.connection:
    """Connect to PostgreSQL with retries."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            conn.autocommit = False
            log.info("Connected to PostgreSQL (attempt %d)", attempt)
            return conn
        except OperationalError as exc:
            log.warning(
                "DB connection failed (attempt %d/%d): %s", attempt, retries, exc
            )
            if attempt < retries:
                time.sleep(delay)

    raise RuntimeError(f"Could not connect to PostgreSQL after {retries} attempts")


def create_table(conn: psycopg2.extensions.connection) -> None:
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
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    log.info("Table abandoned_tokens is ready")


def save_tokens(conn: psycopg2.extensions.connection, tokens: list) -> int:
    """Persist a list of abandoned-token dicts. Returns number of rows inserted."""
    if not tokens:
        return 0

    insert = """
        INSERT INTO abandoned_tokens
            (pair_address, token0, token1, liquidity_usd, detected_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """
    rows = [
        (
            t["pair"],
            t.get("token0"),
            t.get("token1"),
            t.get("liquidity_usd"),
            t.get("timestamp", datetime.utcnow()),
        )
        for t in tokens
    ]

    with conn.cursor() as cur:
        cur.executemany(insert, rows)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------
class ZombieScannerWorker:
    def __init__(self):
        self.conn = None
        self.scanner = PancakeSwapScanner()

    def _ensure_connection(self):
        """Re-connect if the DB connection was lost."""
        try:
            if self.conn and not self.conn.closed:
                self.conn.cursor().execute("SELECT 1")
                return
        except Exception:
            pass

        log.info("(Re)connecting to database...")
        self.conn = connect_db()
        create_table(self.conn)

    def run(self):
        log.info("=== Zombie Scanner Worker starting ===")
        self._ensure_connection()

        cycle = 0
        while True:
            cycle += 1
            log.info("--- Scan cycle #%d ---", cycle)

            try:
                self._ensure_connection()

                abandoned = self.scanner.scan_latest_pairs()
                log.info("Abandoned tokens found this cycle: %d", len(abandoned))

                if abandoned:
                    saved = save_tokens(self.conn, abandoned)
                    log.info("Rows inserted into DB: %d", saved)
                else:
                    log.info("No abandoned tokens detected this cycle")

            except Exception as exc:
                log.error(
                    "Error during scan cycle #%d: %s", cycle, exc, exc_info=True
                )
                # Roll back any open transaction so the connection stays usable
                try:
                    if self.conn and not self.conn.closed:
                        self.conn.rollback()
                except Exception:
                    pass

            log.info("Sleeping %d seconds until next cycle...", SCAN_INTERVAL)
            time.sleep(SCAN_INTERVAL)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    ZombieScannerWorker().run()
