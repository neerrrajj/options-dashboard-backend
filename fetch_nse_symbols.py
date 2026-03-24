#!/usr/bin/env python3
"""
Fetch NSE symbols and save to JSON file.
"""

import json
import requests
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Tuple, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Path to store the symbols JSON file
DATA_DIR = Path(__file__).parent / "data"
SYMBOLS_FILE = DATA_DIR / "nse_symbols.json"
STATUS_FILE = DATA_DIR / "nse_symbols_status.json"

# NSE API endpoints
NSE_EQUITIES_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

# Hardcoded popular indices with their Yahoo Finance symbols
PREDEFINED_INDICES = {
    "^NSEI": "NIFTY 50",
    "^NSEBANK": "BANK NIFTY",
    "^CNXIT": "NIFTY IT",
    "^CNXPHARMA": "NIFTY PHARMA",
    "^CNXAUTO": "NIFTY AUTO",
    "^CNXMETAL": "NIFTY METAL",
    "^CNXREALTY": "NIFTY REALTY",
    "^CNXMEDIA": "NIFTY MEDIA",
    "^CNXENERGY": "NIFTY ENERGY",
    "^CNXFMCG": "NIFTY FMCG",
    "^CNXINFRA": "NIFTY INFRA",
    "^CNXCONSUM": "NIFTY CONSUMPTION",
    "^CNXFIN": "NIFTY FIN SERVICE",
    "^CNXPSUBANK": "NIFTY PSU BANK",
    "^CNXPSE": "NIFTY PSE",
    "^CNXMNC": "NIFTY MNC",
    "^CNXDIVOP": "NIFTY DIV OPPS",
    "^CNX100": "NIFTY 100",
    "^CNX200": "NIFTY 200",
    "^CNX500": "NIFTY 500",
    "^NSEMDCP50": "NIFTY MIDCAP 50",
    "^NSMC100": "NIFTY MIDCAP 100",
    "^NSMC150": "NIFTY MIDCAP 150",
    "^NSC100": "NIFTY SMLCAP 100",
    "^NSC250": "NIFTY SMLCAP 250",
    "^BSESN": "SENSEX",
}


def get_status() -> dict:
    """Get current fetch status."""
    if STATUS_FILE.exists():
        with open(STATUS_FILE, 'r') as f:
            return json.load(f)
    return {
        "last_fetch_date": None,
        "last_fetch_time": None,
        "last_fetch_status": None,
    }


def set_status(status: str, date_str: str = None, time_str: str = None):
    """Update fetch status."""
    status_data = {
        "last_fetch_date": date_str,
        "last_fetch_time": time_str,
        "last_fetch_status": status,
    }
    with open(STATUS_FILE, 'w') as f:
        json.dump(status_data, f, indent=2)


def fetch_nse_equities() -> list:
    """Fetch equity symbols from NSE."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        response = requests.get(NSE_EQUITIES_URL, headers=headers, timeout=30)
        
        if response.status_code != 200:
            logger.warning(f"Failed to fetch from NSE: {response.status_code}")
            return []
        
        # Parse CSV content
        lines = response.text.strip().split('\n')
        symbols = []
        
        # Skip header line
        for line in lines[1:]:
            parts = line.split(',')
            if len(parts) >= 2:
                symbol = parts[0].strip().upper()
                name = parts[1].strip()
                if symbol and name:
                    symbols.append({
                        "symbol": f"{symbol}.NS",
                        "name": name,
                        "type": "equity"
                    })
        
        logger.info(f"Fetched {len(symbols)} equity symbols from NSE")
        return symbols
        
    except Exception as e:
        logger.error(f"Error fetching NSE equities: {e}")
        return []


def create_indices_list() -> list:
    """Create list of indices."""
    indices = []
    for symbol, name in PREDEFINED_INDICES.items():
        indices.append({
            "symbol": symbol,
            "name": name,
            "type": "index"
        })
    return indices


def fetch_all_symbols() -> Tuple[list, dict]:
    """
    Fetch all symbols from NSE and save to JSON.
    Returns (symbols, metadata)
    """
    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)
    
    today = date.today().isoformat()
    now = datetime.now().isoformat()
    
    # Set status to in_progress
    set_status("in_progress", today, now)
    
    try:
        # Fetch equities
        equities = fetch_nse_equities()
        
        # Create indices list
        indices = create_indices_list()
        
        # Combine all symbols
        all_symbols = indices + equities
        
        # Build response with metadata
        result = {
            "metadata": {
                "last_fetch_date": today,
                "last_fetch_time": now,
                "total_symbols": len(all_symbols),
                "indices_count": len(indices),
                "equities_count": len(equities),
            },
            "symbols": all_symbols
        }
        
        # Save to JSON
        with open(SYMBOLS_FILE, 'w') as f:
            json.dump(result, f, indent=2)
        
        # Update status to success
        set_status("success", today, now)
        
        logger.info(f"Saved {len(all_symbols)} symbols")
        return all_symbols, result["metadata"]
        
    except Exception as e:
        logger.error(f"Failed to fetch symbols: {e}")
        set_status("failed", today, now)
        raise


def get_cached_symbols() -> Optional[dict]:
    """Get cached symbols from file."""
    if SYMBOLS_FILE.exists():
        with open(SYMBOLS_FILE, 'r') as f:
            return json.load(f)
    return None


def should_fetch_today() -> bool:
    """Check if we need to fetch fresh symbols today."""
    status = get_status()
    today = date.today().isoformat()
    
    # Fetch if never fetched or last fetch was not today
    if status.get("last_fetch_date") != today:
        return True
    
    return False


def is_fetch_in_progress() -> bool:
    """Check if a fetch is currently in progress."""
    status = get_status()
    return status.get("last_fetch_status") == "in_progress"


if __name__ == "__main__":
    symbols, metadata = fetch_all_symbols()
    print(f"Fetched {metadata['total_symbols']} symbols")
