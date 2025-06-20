import logging
from sqlalchemy import and_, func
from datetime import datetime, timedelta

from db import SessionLocal
from config import INSTRUMENTS
from models import OCSnapshot, OCMinuteSnapshot

logger = logging.getLogger(__name__)

def perform_rollup_for_instrument(instrument_id: str, now_utc: datetime):
    logger.info(f"Rolling up {instrument_id} using snapshot timestamp: {now_utc}")
    db = SessionLocal()
    ist_minute = (now_utc + timedelta(hours=5, minutes=30)).replace(second=0, microsecond=0)

    # Skip if rollup already exists
    # already_rolled_up = db.query(
    #     exists().where(
    #         and_(
    #             OCMinuteSnapshot.instrument == instrument_id,
    #             OCMinuteSnapshot.ist_minute == ist_minute
    #         )
    #     )
    # ).scalar()
    # if already_rolled_up:
    #     logger.info(f"Rollup already exists for {instrument_id} at {ist_minute}, skipping.")
    #     db.close()
    #     return

    # DELETE existing rollup for this minute (if any)
    deleted_count = db.query(OCMinuteSnapshot).filter(
        and_(
            OCMinuteSnapshot.instrument == instrument_id,
            OCMinuteSnapshot.ist_minute == ist_minute
        )
    ).delete()
    
    if deleted_count > 0:
        logger.info(f"Deleted {deleted_count} existing rollup rows for {instrument_id} at {ist_minute}")

    # Get latest snapshot per strike <= now_utc
    subquery = db.query(
        OCSnapshot.strike,
        OCSnapshot.expiry,
        OCSnapshot.option_type,
        func.max(OCSnapshot.snapshot_time).label("max_snapshot_time")
    ).filter(
        OCSnapshot.instrument == instrument_id,
        OCSnapshot.snapshot_time <= now_utc
    ).group_by(
        OCSnapshot.strike,
        OCSnapshot.expiry,
        OCSnapshot.option_type
    ).subquery()

    snapshots = db.query(OCSnapshot).join(
        subquery,
        and_(
            OCSnapshot.strike == subquery.c.strike,
            OCSnapshot.expiry == subquery.c.expiry,
            OCSnapshot.option_type == subquery.c.option_type,
            OCSnapshot.snapshot_time == subquery.c.max_snapshot_time
        )
    ).all()

    if not snapshots:
        logger.warning(f"No snapshots found for {instrument_id} at IST minute {ist_minute} (UTC: {now_utc})")
        db.close()
        return

    instrument = next(i for i in INSTRUMENTS if i["SECURITY_ID"] == instrument_id)
    atm = round(snapshots[0].underlying_price / instrument['STRIKE_RANGE']) * instrument['STRIKE_RANGE']
    lower_bound = atm - 40 * instrument['STRIKE_RANGE']
    upper_bound = atm + 40 * instrument['STRIKE_RANGE']

    combined_rows = {}

    for snap in snapshots:
        if not (lower_bound <= snap.strike <= upper_bound):
            continue

        key = (snap.strike, snap.expiry)
        if key not in combined_rows:
            combined_rows[key] = {
                "strike": snap.strike,
                "expiry": snap.expiry,
                "instrument": snap.instrument,
                "underlying_price": snap.underlying_price,
                "call_delta": None, "call_theta": None, "call_gamma": None,
                "call_vega": None, "call_iv": None, "call_oi": None,
                "call_last_price": None, "call_volume": None,
                "put_delta": None, "put_theta": None, "put_gamma": None,
                "put_vega": None, "put_iv": None, "put_oi": None,
                "put_last_price": None, "put_volume": None,
            }

        row = combined_rows[key]

        if snap.option_type == "CE":
            row.update({
                "call_delta": snap.delta,
                "call_theta": snap.theta,
                "call_gamma": snap.gamma,
                "call_vega": snap.vega,
                "call_iv": snap.iv,
                "call_oi": snap.oi,
                "call_last_price": snap.last_price,
                "call_volume": snap.volume,
            })
        elif snap.option_type == "PE":
            row.update({
                "put_delta": snap.delta,
                "put_theta": snap.theta,
                "put_gamma": snap.gamma,
                "put_vega": snap.vega,
                "put_iv": snap.iv,
                "put_oi": snap.oi,
                "put_last_price": snap.last_price,
                "put_volume": snap.volume,
            })

    for (strike, expiry), row_data in combined_rows.items():
        row = OCMinuteSnapshot(
            timestamp=now_utc,
            ist_minute=ist_minute,
            instrument=instrument_id,
            expiry=expiry,
            strike=strike,
            underlying_price=row_data["underlying_price"],
            call_delta=row_data["call_delta"],
            call_theta=row_data["call_theta"],
            call_gamma=row_data["call_gamma"],
            call_vega=row_data["call_vega"],
            call_iv=row_data["call_iv"],
            call_oi=row_data["call_oi"],
            call_last_price=row_data["call_last_price"],
            call_volume=row_data["call_volume"],
            put_delta=row_data["put_delta"],
            put_theta=row_data["put_theta"],
            put_gamma=row_data["put_gamma"],
            put_vega=row_data["put_vega"],
            put_iv=row_data["put_iv"],
            put_oi=row_data["put_oi"],
            put_last_price=row_data["put_last_price"],
            put_volume=row_data["put_volume"],
        )
        db.add(row)

    db.commit()
    db.close()
    logger.info(f"Rolled up {len(combined_rows)} rows for {instrument_id} at {ist_minute}")

