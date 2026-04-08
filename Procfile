worker: python main.py
web: uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}
liquidity-updater: python -m liquidity_updater
