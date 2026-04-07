"""
PancakeSwap abandoned-token scanner.

Flow:
  1. Connect to BSC via Web3 (public RPC, no key required).
  2. Call PancakeSwap V2 Factory to get the last 50 pair addresses.
  3. For each pair, query DexScreener to get real USD liquidity.
  4. Flag pairs whose liquidity_usd < LIQUIDITY_THRESHOLD as abandoned.
  5. Return a list of dicts ready to be persisted by the worker.
"""

import logging
import time
from datetime import datetime, timezone

import requests
from web3 import Web3

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BSC_RPC = "https://bsc-dataseed.binance.org/"

# PancakeSwap V2 Factory on BSC
PANCAKE_FACTORY = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"

# Minimal ABI – only the two methods we need
FACTORY_ABI = [
    {
        "inputs": [],
        "name": "allPairsLength",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "name": "allPairs",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

PAIR_ABI = [
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

DEXSCREENER_URL = "https://api.dexscreener.com/latest/dex/pairs/bsc/{address}"

PAIRS_TO_FETCH = 50          # how many of the latest pairs to inspect
LIQUIDITY_THRESHOLD = 5000   # USD – below this is considered abandoned
REQUEST_TIMEOUT = 10         # seconds for HTTP calls
RATE_LIMIT_DELAY = 0.5       # seconds between DexScreener requests


# ---------------------------------------------------------------------------
# Scanner class
# ---------------------------------------------------------------------------
class PancakeSwapScanner:
    def __init__(self):
        self.w3 = Web3(Web3.HTTPProvider(BSC_RPC, request_kwargs={"timeout": 15}))
        self.factory = self.w3.eth.contract(
            address=Web3.to_checksum_address(PANCAKE_FACTORY),
            abi=FACTORY_ABI,
        )
        self._check_connection()

    # ------------------------------------------------------------------
    def _check_connection(self):
        if self.w3.is_connected():
            log.info("Web3 connected to BSC node: %s", BSC_RPC)
        else:
            log.warning("Web3 could NOT connect to BSC node: %s", BSC_RPC)

    # ------------------------------------------------------------------
    def _get_latest_pair_addresses(self) -> list:
        """Return the last PAIRS_TO_FETCH pair addresses from the factory."""
        try:
            total = self.factory.functions.allPairsLength().call()
            log.info("PancakeSwap total pairs: %d", total)
        except Exception as exc:
            log.error("Failed to read allPairsLength: %s", exc)
            return []

        start = max(0, total - PAIRS_TO_FETCH)
        addresses = []
        for i in range(start, total):
            try:
                addr = self.factory.functions.allPairs(i).call()
                addresses.append(Web3.to_checksum_address(addr))
            except Exception as exc:
                log.warning("Could not fetch pair index %d: %s", i, exc)

        log.info("Fetched %d pair addresses from factory", len(addresses))
        return addresses

    # ------------------------------------------------------------------
    def _get_token_addresses(self, pair_address: str):
        """Return (token0, token1) for a pair contract, or (None, None) on error."""
        try:
            pair_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(pair_address),
                abi=PAIR_ABI,
            )
            t0 = pair_contract.functions.token0().call()
            t1 = pair_contract.functions.token1().call()
            return t0.lower(), t1.lower()
        except Exception as exc:
            log.warning("Could not read tokens for pair %s: %s", pair_address, exc)
            return None, None

    # ------------------------------------------------------------------
    def _get_liquidity_usd(self, pair_address: str) -> float:
        """Query DexScreener for the USD liquidity of a pair. Returns 0.0 on error."""
        url = DEXSCREENER_URL.format(address=pair_address.lower())
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            pairs = data.get("pairs") or []
            if not pairs:
                return 0.0
            liquidity = pairs[0].get("liquidity") or {}
            return float(liquidity.get("usd") or 0.0)
        except requests.exceptions.Timeout:
            log.warning("DexScreener timeout for pair %s", pair_address)
            return 0.0
        except Exception as exc:
            log.warning("DexScreener error for pair %s: %s", pair_address, exc)
            return 0.0

    # ------------------------------------------------------------------
    def scan_latest_pairs(self) -> list:
        """
        Main entry point.

        Returns a list of dicts for pairs whose liquidity_usd < LIQUIDITY_THRESHOLD:
            {
                "pair":          str,   # checksummed pair address
                "token0":        str,   # lowercase token0 address
                "token1":        str,   # lowercase token1 address
                "liquidity_usd": float,
                "timestamp":     datetime (UTC),
            }
        """
        log.info("Starting scan of latest %d PancakeSwap pairs...", PAIRS_TO_FETCH)
        pair_addresses = self._get_latest_pair_addresses()

        if not pair_addresses:
            log.warning("No pair addresses retrieved – skipping scan")
            return []

        abandoned = []
        now = datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC for psycopg2

        for idx, pair_addr in enumerate(pair_addresses, start=1):
            log.debug("[%d/%d] Checking pair %s", idx, len(pair_addresses), pair_addr)

            liquidity_usd = self._get_liquidity_usd(pair_addr)
            log.info(
                "[%d/%d] Pair %s  liquidity_usd=%.2f",
                idx,
                len(pair_addresses),
                pair_addr,
                liquidity_usd,
            )

            if liquidity_usd < LIQUIDITY_THRESHOLD:
                token0, token1 = self._get_token_addresses(pair_addr)
                abandoned.append(
                    {
                        "pair": pair_addr,
                        "token0": token0,
                        "token1": token1,
                        "liquidity_usd": liquidity_usd,
                        "timestamp": now,
                    }
                )
                log.info(
                    "  --> ABANDONED  token0=%s  token1=%s  liquidity_usd=%.2f",
                    token0,
                    token1,
                    liquidity_usd,
                )

            # Rate-limit DexScreener requests
            time.sleep(RATE_LIMIT_DELAY)

        log.info(
            "Scan complete. %d / %d pairs flagged as abandoned (threshold $%d)",
            len(abandoned),
            len(pair_addresses),
            LIQUIDITY_THRESHOLD,
        )
        return abandoned
