from typing import List
from zoneinfo import ZoneInfo
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct, cast, Date
from datetime import datetime, date, time
from fastapi import APIRouter, Depends, Query

from db import get_db
from schemas import OTMGreeksResponse
from models import OCSummary, HistoricalOCSummary

router = APIRouter(prefix="/api/greeks", tags=["Greeks"])

IST = ZoneInfo("Asia/Kolkata")

@router.get("/defaults")
def greeks_defaults(
    instrument: str = "NIFTY",
    live: bool = True,
    db: Session = Depends(get_db)
):
    """Get default expiry and current date for an instrument"""
    model = OCSummary if live else HistoricalOCSummary
    
    # Get nearest expiry for the instrument
    nearest_expiry = db.query(model.expiry).filter(
        model.instrument == instrument
    ).order_by(model.expiry).first()
    
    # Get current IST date
    current_date = datetime.now(IST).date().isoformat()

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
def greeks_metadata(
    instrument: str = None,
    live: bool = True,
    date: str = None,
    db: Session = Depends(get_db)
):
    """Enhanced metadata with dependency chain: Mode → Date → Instrument → Expiry"""
    model = OCSummary if live else HistoricalOCSummary
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

@router.get("/summary")
def greeks_summary(
    instrument: str,
    expiry: date,
    live: bool = True,
    date_ist: date = Query(...),
    db: Session = Depends(get_db)
):
    model = OCSummary if live else HistoricalOCSummary
    start_time_ist = datetime.combine(date_ist, time.min).replace(tzinfo=IST)
    end_time_ist = datetime.combine(date_ist, time.max).replace(tzinfo=IST)
    query = db.query(model).filter(
        model.instrument == instrument,
        model.expiry == expiry,
        model.ist_minute >= start_time_ist,
        model.ist_minute <= end_time_ist
    ).order_by(model.ist_minute)

    return query.all()
