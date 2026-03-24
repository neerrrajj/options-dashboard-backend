from typing import List, Optional
import time
from fastapi import APIRouter, Query
from pydantic import BaseModel

from fetch_nse_symbols import (
    fetch_all_symbols, 
    get_cached_symbols, 
    get_status,
    should_fetch_today,
    is_fetch_in_progress,
    set_status
)

router = APIRouter(prefix="/api/symbols", tags=["Symbols"])


class SymbolEntry(BaseModel):
    symbol: str
    name: str
    type: str  # "index" or "equity"


class SymbolsResponse(BaseModel):
    metadata: dict
    symbols: List[SymbolEntry]


@router.get("/list", response_model=SymbolsResponse)
def get_symbols_list(
    search: Optional[str] = Query(None, description="Search query for symbol or name")
):
    """
    Get list of NSE symbols.
    
    - First request of day: Fetches fresh from NSE (blocking)
    - Subsequent requests: Returns cached data
    - **search**: Optional filter on symbol or company name (client-side)
    """
    # Check if we need to fetch fresh data
    need_fetch = should_fetch_today()
    
    if need_fetch:
        # Check if another request is already fetching
        if is_fetch_in_progress():
            # Wait for in-progress fetch to complete (max 30 seconds)
            for _ in range(30):
                time.sleep(1)
                if not is_fetch_in_progress():
                    break
            # Fall through to return cached data
        else:
            # Fetch fresh data (blocking)
            try:
                fetch_all_symbols()
            except Exception:
                # If fetch fails, continue to return stale data
                pass
    
    # Get cached symbols (fresh or stale)
    cached = get_cached_symbols()
    
    if cached is None:
        # No cache at all - try to fetch even if not needed
        try:
            fetch_all_symbols()
            cached = get_cached_symbols()
        except Exception as e:
            return {
                "metadata": {"error": str(e)},
                "symbols": []
            }
    
    symbols = cached.get("symbols", [])
    metadata = cached.get("metadata", {})
    
    # Add current status to metadata
    status = get_status()
    metadata["fetch_status"] = status.get("last_fetch_status")
    
    # Filter by search if provided (client-side search)
    if search:
        search_lower = search.lower()
        filtered = []
        for s in symbols:
            if (search_lower in s["symbol"].lower() or 
                search_lower in s["name"].lower()):
                filtered.append(s)
        symbols = filtered[:50]  # Limit to 50 results
    
    return {
        "metadata": metadata,
        "symbols": symbols
    }


@router.get("/status")
def get_symbols_status():
    """Get current fetch status."""
    return get_status()


@router.get("/refresh")
def refresh_symbols():
    """Force refresh symbols from NSE (admin only in production)."""
    try:
        symbols, metadata = fetch_all_symbols()
        return {
            "message": "Symbols refreshed successfully",
            "metadata": metadata
        }
    except Exception as e:
        return {
            "message": f"Failed to refresh: {str(e)}",
            "error": str(e)
        }
