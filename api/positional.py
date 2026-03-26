"""Positional analysis API - Yahoo Finance data and statistics."""

import logging
from datetime import datetime, date, timedelta
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import yfinance as yf
import pandas as pd
import numpy as np

try:
    # Try to use curl_cffi for better TLS impersonation
    from curl_cffi import requests as curl_requests
    _session = curl_requests.Session(impersonate="chrome120")
    logging.info("[POSITIONAL] Using curl_cffi for Yahoo Finance requests")
except ImportError:
    # Fallback to regular requests with headers
    import requests
    _session = requests.Session()
    _session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    logging.info("[POSITIONAL] Using regular requests for Yahoo Finance")

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/positional", tags=["Positional"])


class StatsRequest(BaseModel):
    symbol: str
    start_date: str  # ISO format: YYYY-MM-DD
    end_date: str    # ISO format: YYYY-MM-DD
    frequency: str = "Daily"  # Daily, Weekly, Monthly
    week_start_day: Optional[str] = "Monday"
    month_start_day: Optional[int] = 1


class MetricStats(BaseModel):
    min: float
    max: float
    avg: float
    range_68_lower: float
    range_68_upper: float
    range_95_lower: float
    range_95_upper: float
    range_99_lower: float
    range_99_upper: float


class MetricData(BaseModel):
    points: MetricStats
    percentage: MetricStats


class GapData(BaseModel):
    gap_up: MetricData
    gap_down: MetricData


class RangeData(BaseModel):
    total_range: MetricData
    body_range: MetricData


class NetChangeData(BaseModel):
    from_prev_close: MetricData
    from_open: MetricData


class StatsResponse(BaseModel):
    metadata: dict
    range: RangeData
    net_change: NetChangeData
    gaps: GapData


def calc_stats(series: pd.Series, is_always_positive: bool = False) -> dict:
    """Calculate statistics with standard deviation ranges."""
    series_clean = series.replace([np.inf, -np.inf], np.nan).dropna()
    
    if len(series_clean) == 0:
        return {
            'min': 0, 'max': 0, 'avg': 0,
            'range_68_lower': 0, 'range_68_upper': 0,
            'range_95_lower': 0, 'range_95_upper': 0,
            'range_99_lower': 0, 'range_99_upper': 0,
        }
    
    mean_val = series_clean.mean()
    std_val = series_clean.std()
    
    # Handle NaN or zero std
    if pd.isna(std_val) or std_val == 0:
        std_val = 0
    
    result = {
        'min': float(series_clean.min()),
        'max': float(series_clean.max()),
        'avg': float(mean_val),
        'range_68_lower': float(mean_val - std_val),
        'range_68_upper': float(mean_val + std_val),
        'range_95_lower': float(mean_val - 2 * std_val),
        'range_95_upper': float(mean_val + 2 * std_val),
        'range_99_lower': float(mean_val - 3 * std_val),
        'range_99_upper': float(mean_val + 3 * std_val),
    }
    
    # Ensure non-negative for always-positive metrics
    if is_always_positive:
        for key in result:
            if 'lower' in key and result[key] < 0:
                result[key] = 0
    
    return result


def resample_data(data: pd.DataFrame, frequency: str, week_start_day: str, month_start_day: int) -> pd.DataFrame:
    """Resample data to weekly or monthly frequency."""
    if frequency == "Daily":
        return data
    
    if frequency == "Weekly":
        day_map = {
            'Monday': 'W-MON', 'Tuesday': 'W-TUE', 'Wednesday': 'W-WED',
            'Thursday': 'W-THU', 'Friday': 'W-FRI', 'Saturday': 'W-SAT', 'Sunday': 'W-SUN'
        }
        freq = day_map.get(week_start_day, 'W-MON')
        
        resampled = data.resample(freq).agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).dropna()
        
        return resampled
    
    elif frequency == "Monthly":
        # Monthly resampling
        resampled = data.resample('MS').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).dropna()
        
        return resampled
    
    return data


def calculate_statistics(df: pd.DataFrame, start_date: date = None) -> dict:
    """Calculate all statistics from price data."""
    if df.empty:
        return {}
    
    # Calculate metrics
    df['Total_Range_Points'] = df['High'] - df['Low']
    df['Total_Range_Pct'] = (df['Total_Range_Points'] / df['Open']) * 100
    
    df['Body_Points'] = (df['Close'] - df['Open']).abs()
    df['Body_Pct'] = (df['Body_Points'] / df['Open']) * 100
    
    # Net change
    df['Prev_Close'] = df['Close'].shift(1)
    df['Net_Change_From_Prev_Points'] = df['Close'] - df['Prev_Close']
    df['Net_Change_From_Prev_Pct'] = (df['Net_Change_From_Prev_Points'] / df['Prev_Close']) * 100
    
    df['Net_Change_From_Open_Points'] = df['Close'] - df['Open']
    df['Net_Change_From_Open_Pct'] = (df['Net_Change_From_Open_Points'] / df['Open']) * 100
    
    # Gaps
    df['Gap_Points'] = df['Open'] - df['Prev_Close']
    df['Gap_Pct'] = (df['Gap_Points'] / df['Prev_Close']) * 100
    
    # Remove first row (no prev_close)
    df = df.dropna()
    
    # Filter to actual start date (remove buffer day used for calculations)
    if start_date:
        df = df[df.index.date >= start_date]
    
    if df.empty:
        return {}
    
    stats = {}
    
    # Range statistics - Total and Body
    stats['range'] = {
        'total_range': {
            'points': calc_stats(df['Total_Range_Points'], is_always_positive=True),
            'percentage': calc_stats(df['Total_Range_Pct'], is_always_positive=True)
        },
        'body_range': {
            'points': calc_stats(df['Body_Points'], is_always_positive=True),
            'percentage': calc_stats(df['Body_Pct'], is_always_positive=True)
        }
    }
    
    # Net change - From Prev Close and From Open
    stats['net_change'] = {
        'from_prev_close': {
            'points': calc_stats(df['Net_Change_From_Prev_Points'], is_always_positive=False),
            'percentage': calc_stats(df['Net_Change_From_Prev_Pct'], is_always_positive=False)
        },
        'from_open': {
            'points': calc_stats(df['Net_Change_From_Open_Points'], is_always_positive=False),
            'percentage': calc_stats(df['Net_Change_From_Open_Pct'], is_always_positive=False)
        }
    }
    
    # Gap up (positive gaps only)
    gap_up_data = df[df['Gap_Points'] > 0]
    if not gap_up_data.empty:
        gap_up_stats = {
            'points': calc_stats(gap_up_data['Gap_Points'], is_always_positive=True),
            'percentage': calc_stats(gap_up_data['Gap_Pct'], is_always_positive=True)
        }
    else:
        gap_up_stats = {
            'points': calc_stats(pd.Series([0]), is_always_positive=True),
            'percentage': calc_stats(pd.Series([0]), is_always_positive=True)
        }
    
    # Gap down (negative gaps as positive values)
    gap_down_data = df[df['Gap_Points'] < 0]
    if not gap_down_data.empty:
        gap_down_stats = {
            'points': calc_stats(gap_down_data['Gap_Points'].abs(), is_always_positive=True),
            'percentage': calc_stats(gap_down_data['Gap_Pct'].abs(), is_always_positive=True)
        }
    else:
        gap_down_stats = {
            'points': calc_stats(pd.Series([0]), is_always_positive=True),
            'percentage': calc_stats(pd.Series([0]), is_always_positive=True)
        }
    
    stats['gaps'] = {
        'gap_up': gap_up_stats,
        'gap_down': gap_down_stats
    }
    
    # Add metadata
    stats['metadata'] = {
        'total_candles': len(df),
        'gap_up_candles': len(gap_up_data),
        'gap_down_candles': len(gap_down_data),
        'no_gap_candles': len(df) - len(gap_up_data) - len(gap_down_data),
    }
    
    return stats


@router.post("/stats", response_model=StatsResponse)
def get_positional_stats(request: StatsRequest):
    """
    Get positional statistics for a symbol.
    
    - symbol: Yahoo Finance symbol (e.g., ^NSEI for NIFTY 50)
    - start_date: Start date (YYYY-MM-DD)
    - end_date: End date (YYYY-MM-DD)
    - frequency: Daily, Weekly, or Monthly
    """
    try:
        # Parse dates
        start_date = datetime.strptime(request.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(request.end_date, "%Y-%m-%d").date()
        
        # Add buffer for calculations (need 1 extra day for prev_close calculations)
        buffer_start = start_date - timedelta(days=10)
        analysis_start = start_date - timedelta(days=1)
        
        logger.info(f"[POSITIONAL] Fetching data for {request.symbol} from {buffer_start} to {end_date}")
        
        # Fetch data from Yahoo Finance using custom session with browser headers
        ticker = yf.Ticker(request.symbol, session=_session)
        data = ticker.history(start=buffer_start, end=end_date)
        
        if data.empty:
            raise HTTPException(status_code=404, detail=f"No data found for symbol: {request.symbol}")
        
        logger.info(f"[POSITIONAL] Fetched {len(data)} rows for {request.symbol}")
        
        # Resample if needed
        data = resample_data(data, request.frequency, request.week_start_day or "Monday", request.month_start_day or 1)
        
        # Filter to requested date range (keep 1 extra day for prev_close calculations)
        data = data[data.index.date >= analysis_start]
        
        if data.empty:
            raise HTTPException(status_code=404, detail="No data in selected date range")
        
        # Calculate statistics
        stats = calculate_statistics(data, start_date)
        
        if not stats:
            raise HTTPException(status_code=500, detail="Failed to calculate statistics")
        
        # Build response
        response = {
            'metadata': {
                'symbol': request.symbol,
                'start_date': request.start_date,
                'end_date': request.end_date,
                'frequency': request.frequency,
                **stats['metadata']
            },
            'range': stats['range'],
            'net_change': stats['net_change'],
            'gaps': stats['gaps']
        }
        
        logger.info(f"[POSITIONAL] Calculated stats for {request.symbol}: {stats['metadata']}")
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[POSITIONAL] Error calculating stats: {e}")
        raise HTTPException(status_code=500, detail=f"Error calculating statistics: {str(e)}")
