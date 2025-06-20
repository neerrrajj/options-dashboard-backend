import uuid
from datetime import datetime
from sqlalchemy import Column, String, Float, DateTime, Date, BigInteger, Index
from sqlalchemy.dialects.postgresql import UUID

from db import Base

class OCSnapshot(Base):
    __tablename__ = "oc_snapshots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    timestamp = Column(DateTime, default=datetime.utcnow)
    snapshot_time = Column(DateTime, nullable=False)
    instrument = Column(String)
    expiry = Column(Date)
    underlying_price = Column(Float)
    strike = Column(Float)
    option_type = Column(String)  # 'CE' or 'PE'
    delta = Column(Float)
    theta = Column(Float)
    gamma = Column(Float)
    vega = Column(Float)
    iv = Column(Float)
    oi = Column(BigInteger)
    last_price = Column(Float)
    volume = Column(BigInteger)

class OCMinuteSnapshot(Base):
    __tablename__ = "oc_minute_snapshots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    timestamp = Column(DateTime, default=datetime.utcnow)
    ist_minute = Column(DateTime, index=True, nullable=False)  # IST rounded minute
    instrument = Column(String, index=True)
    expiry = Column(Date, index=True)
    underlying_price = Column(Float)
    strike = Column(Float)

    call_delta = Column(Float)
    call_theta = Column(Float)
    call_gamma = Column(Float)
    call_vega = Column(Float)
    call_iv = Column(Float)
    call_oi = Column(BigInteger)
    call_volume = Column(BigInteger)
    call_last_price = Column(Float)

    put_delta = Column(Float)
    put_theta = Column(Float)
    put_gamma = Column(Float)
    put_vega = Column(Float)
    put_iv = Column(Float)
    put_oi = Column(BigInteger)
    put_volume = Column(BigInteger)
    put_last_price = Column(Float)

    call_gex = Column(Float)
    put_gex = Column(Float)
    net_gex = Column(Float)
    abs_gex = Column(Float)
    

    __table_args__ = (
        Index('ix_minute_snapshot_unique', "timestamp", "instrument", "expiry", "strike", unique=True),
    )
