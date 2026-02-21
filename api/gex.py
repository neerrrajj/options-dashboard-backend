from zoneinfo import ZoneInfo
from typing import List, Optional
from sqlalchemy.orm import Session
from datetime import datetime, date
from fastapi import APIRouter, Query, Depends
from sqlalchemy import func, distinct, cast, Date

from db import get_db
from models import OCMinuteSnapshot, HistoricalOCSnapshot
from schemas import GEXSummaryResponse, TopStrikesResponse, ChartDataResponse

router = APIRouter(prefix="/api/gex", tags=["GEX"])

IST = ZoneInfo("Asia/Kolkata")

@router.get("/defaults")
def gex_defaults(
    instrument: str = "NIFTY",
    live: bool = True,
    db: Session = Depends(get_db)
):
    """Get default expiry and current date for an instrument"""
    model = OCMinuteSnapshot if live else HistoricalOCSnapshot
    current_date = datetime.now(IST).date().isoformat()
    
    # Get nearest expiry for the instrument
    nearest_expiry = db.query(model.expiry).filter(
        model.instrument == instrument
    ).order_by(model.expiry).first()
    
    # For historical mode, get the latest available date
    latest_historical_date = current_date
    if not live:
        date_column = cast(func.date(model.ist_minute), Date).label("ist_date")
        latest_date = db.query(func.max(date_column)).scalar()
        if latest_date:
            latest_historical_date = latest_date.isoformat()
    
    return {
        "instrument": instrument,
        "expiry": nearest_expiry[0].isoformat() if nearest_expiry else None,
        "date": latest_historical_date if not live else current_date,
        "current_date": current_date
    }

@router.get("/metadata")
def gex_metadata(
    instrument: str = None,
    live: bool = True,
    date: str = None,
    db: Session = Depends(get_db)
):
    """Enhanced metadata with dependency chain: Mode → Date → Instrument → Expiry"""
    model = OCMinuteSnapshot if live else HistoricalOCSnapshot
    current_date = datetime.now(IST).date().isoformat()
    
    # Step 1: Get all available dates (for historical mode)
    available_dates = []
    if not live:  # Only for historical mode
        date_column = cast(func.date(model.ist_minute), Date).label("ist_date")
        dates = db.query(distinct(date_column)).order_by(func.date(model.ist_minute)).all()
        available_dates = [d[0].isoformat() for d in dates]
    
    # Step 2: Get instruments available for the selected date
    instruments = []
    if live:
        # For live mode, get all instruments
        instruments_query = db.query(distinct(model.instrument)).order_by(model.instrument)
        instruments = [i[0] for i in instruments_query.all()]
    else:
        # For historical mode, get instruments available on selected date
        if date:
            try:
                selected_date = datetime.fromisoformat(date).date()
                date_column = cast(func.date(model.ist_minute), Date)
                instruments_query = db.query(distinct(model.instrument)).filter(
                    date_column == selected_date
                ).order_by(model.instrument)
                instruments = [i[0] for i in instruments_query.all()]
            except (ValueError, TypeError):
                instruments = []
        else:
            # If no date selected, return all instruments
            instruments_query = db.query(distinct(model.instrument)).order_by(model.instrument)
            instruments = [i[0] for i in instruments_query.all()]
    
    # Step 3: Get expiries for selected instrument and date
    expiries = []
    nearest_expiry = None
    
    if instrument:
        if live:
            # For live mode, get all expiries for instrument
            expiries_query = db.query(distinct(model.expiry)).filter(
                model.instrument == instrument
            ).order_by(model.expiry)
            expiries = [e[0].isoformat() for e in expiries_query.all()]
        else:
            # For historical mode, get expiries available on selected date
            if date:
                try:
                    selected_date = datetime.fromisoformat(date).date()
                    date_column = cast(func.date(model.ist_minute), Date)
                    expiries_query = db.query(distinct(model.expiry)).filter(
                        model.instrument == instrument,
                        date_column == selected_date
                    ).order_by(model.expiry)
                    expiries = [e[0].isoformat() for e in expiries_query.all()]
                except (ValueError, TypeError):
                    expiries = []
        
        # Get the latest expiry
        nearest_expiry = expiries[0] if expiries else None
    
    return {
        "instruments": instruments,
        "expiries": expiries,
        "nearest_expiry": nearest_expiry,
        "available_dates": available_dates,
        "current_date": current_date
    }

@router.get("/timestamps")
def get_available_timestamps(
    instrument: str,
    expiry: Optional[date] = None,
    date: str = Query(...),
    live: bool = True,
    db: Session = Depends(get_db)
):
    model = OCMinuteSnapshot if live else HistoricalOCSnapshot
    
    # Parse the date and set time range
    target_date = datetime.strptime(date, '%Y-%m-%d').date()
    
    if live:
        # For live mode, get all data for today
        start_time = datetime.combine(target_date, datetime.min.time().replace(hour=9, minute=15))
        end_time = datetime.now()  # Current time
    else:
        # For historical mode, get all data for the specified date
        start_time = datetime.combine(target_date, datetime.min.time().replace(hour=9, minute=15))
        end_time = datetime.combine(target_date, datetime.min.time().replace(hour=15, minute=30))
    
    # Get all available timestamps
    timestamps_query = db.query(model.ist_minute.distinct()).filter(
        model.instrument == instrument,
        model.ist_minute >= start_time,
        model.ist_minute <= end_time
    )
    
    if expiry:
        timestamps_query = timestamps_query.filter(model.expiry == expiry)
    
    timestamps = timestamps_query.order_by(model.ist_minute).all()
    timestamp_list = [ts[0] for ts in timestamps]
    
    # Get the latest timestamp
    latest_timestamp = timestamp_list[-1] if timestamp_list else None
    
    return {
        "timestamps": [ts.isoformat() for ts in timestamp_list],
        "latest_timestamp": latest_timestamp.isoformat() if latest_timestamp else None
    }

@router.get("/data")
def gex_summary(
    instrument: str,
    expiry: Optional[date] = None,
    live: bool = True,
    start_time: datetime = Query(...),
    end_time: datetime = Query(...),
    db: Session = Depends(get_db)
):
    model = OCMinuteSnapshot if live else HistoricalOCSnapshot
    query = db.query(model).filter(
        model.instrument == instrument,
        model.ist_minute >= start_time,
        model.ist_minute <= end_time
    )
    if expiry:
        query = query.filter(model.expiry == expiry)
    return query.order_by(model.ist_minute).all()
