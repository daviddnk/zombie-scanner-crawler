import logging
import time
from web3 import Web3
import requests
import psycopg2
from psycopg2 import sql

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database configuration
DB_HOST = 'localhost'
DB_NAME = 'your_database'
DB_USER = 'your_user'
DB_PASS = 'your_password'

# Initialize Web3
BSC_NODE = 'https://bsc-dataseed.binance.org/'
w3 = Web3(Web3.HTTPProvider(BSC_NODE))

# Function to connect to PostgreSQL database
def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )
    return conn

# Function to create table if not exists
def create_table():
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL(""
                    CREATE TABLE IF NOT EXISTS abandoned_tokens (
                        id SERIAL PRIMARY KEY,
                        token_address VARCHAR(42) NOT NULL,
                        symbol VARCHAR(10),
                        name VARCHAR(255),
                        price FLOAT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            )
    logging.info('Database table created or verified.')

# Function to save token to database
def save_token(token):
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("INSERT INTO abandoned_tokens (token_address, symbol, name, price) VALUES (%s, %s, %s, %s)"),
                (token['address'], token['symbol'], token['name'], token['price'])
            )
    logging.info('Token saved to database: %s', token['address'])

# Function to check abandoned tokens
def check_abandoned_tokens():
    logging.info('Checking for abandoned tokens...')
    # Your logic to call DexScreener API and filter abandoned tokens goes here
    # For example:
    response = requests.get('https://api.dexscreener.com/latest/dex/pairs/bsc')
    if response.status_code != 200:
        logging.error('Failed to fetch pairs from DexScreener')
        return

    pairs = response.json().get('pairs', [])
    for pair in pairs:
        # Placeholder logic to detect abandoned tokens
        if pair['liquidity'] < 100:
            save_token({'address': pair['address'], 'symbol': pair['symbol'], 'name': pair['name'], 'price': pair['price']})

# Main loop
if __name__ == '__main__':
    create_table()
    while True:
        try:
            check_abandoned_tokens()
            time.sleep(60)  # Adjust interval as needed
        except Exception as e:
            logging.error('Error in main loop: %s', str(e))
            time.sleep(5)  # Wait before retrying
