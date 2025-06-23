import os
from dotenv import load_dotenv
from datetime import timedelta

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
DHAN_API_URL = os.getenv("DHAN_API_URL")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")

IST_OFFSET = timedelta(hours=5, minutes=30)

INSTRUMENTS = [
    {
        "SECURITY_ID": "NIFTY",
        "UNDERLYING_SYMBOL": 13,
        "UNDERLYING_SEGMENT": "IDX_I",
        "LOT_SIZE": 75,
        "STRIKE_RANGE": 50,
        "EXPIRIES": 7
    }, {
        "SECURITY_ID": "BANKNIFTY",
        "UNDERLYING_SYMBOL": 25,
        "UNDERLYING_SEGMENT": "IDX_I",
        "LOT_SIZE": 25,
        "STRIKE_RANGE": 100,
        "EXPIRIES": 3
    }
]

HOLIDAYS = [
    "2025-08-15",
    "2025-08-27",
    "2025-10-02",
    "2025-10-21",
    "2025-10-22",
    "2025-11-05",
    "2025-12-25"
]