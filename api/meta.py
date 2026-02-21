# from datetime import date
# from sqlalchemy.orm import Session
# from fastapi import APIRouter, Depends
# from typing import Dict, List, Optional

# from db import get_db
# from models import OCSummary, HistoricalOCSummary
# from schemas import InstrumentExpiryMap, LatestMinuteResponse

# router = APIRouter(prefix="/api", tags=["Meta"])


# @router.get("/instruments", response_model=InstrumentExpiryMap)
# def get_instruments_expiries(db: Session = Depends(get_db)):
#     rows = db.query(OCSummary.instrument, OCSummary.expiry).distinct().all()
#     instrument_map: Dict[str, List[date]] = {}
#     for instrument, expiry in rows:
#         instrument_map.setdefault(instrument, []).append(expiry)
#     return instrument_map


# @router.get("/latest-minute", response_model=LatestMinuteResponse)
# def get_latest_minute(
#     instrument: str,
#     expiry: Optional[date] = None,
#     live: bool = True,
#     db: Session = Depends(get_db)
# ):
#     model = OCSummary if live else HistoricalOCSummary
#     query = db.query(model.ist_minute).filter(model.instrument == instrument)
#     if expiry:
#         query = query.filter(model.expiry == expiry)
#     latest = query.order_by(model.ist_minute.desc()).first()
#     return {"latest_minute": latest[0] if latest else None}
