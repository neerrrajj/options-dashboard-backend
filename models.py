import uuid
from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import Column, String, Float, DateTime, Date, BigInteger, Index

from db import Base

class OCMinuteSnapshot(Base):
    __tablename__ = "oc_minute_snapshots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    timestamp = Column(DateTime, default=datetime.utcnow)
    ist_minute = Column(DateTime, index=True, nullable=False)
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
        Index('ix_snapshots_minute_instrument', "ist_minute", "instrument"),
        Index('ix_snapshots_netgex', "ist_minute", "instrument", "net_gex"),
        Index('ix_snapshots_absgex', "ist_minute", "instrument", "abs_gex"),
    )

class OCSummary(Base):
    __tablename__ = "oc_summary"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    timestamp = Column(DateTime, default=datetime.utcnow)
    ist_minute = Column(DateTime, index=True, nullable=False)
    instrument = Column(String, index=True)
    expiry = Column(Date, index=True)
    underlying_price = Column(Float)

    total_net_gex = Column(Float)
    gamma_flip_level = Column(Float)
    otm_call_vega = Column(Float)
    otm_put_vega = Column(Float)
    otm_call_theta = Column(Float)
    otm_put_theta = Column(Float)
    otm_call_delta = Column(Float)
    otm_put_delta = Column(Float)

    __table_args__ = (
        Index("ix_summary_minute_instrument", "ist_minute", "instrument"),
    )

class HistoricalOCSnapshot(Base):
    __tablename__ = "historical_oc_snapshots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    timestamp = Column(DateTime, default=datetime.utcnow)
    ist_minute = Column(DateTime, index=True, nullable=False)
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
        Index('ix_hist_snapshots_minute_instrument', "ist_minute", "instrument"),
    )

class HistoricalOCSummary(Base):
    __tablename__ = "historical_oc_summary"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    timestamp = Column(DateTime, default=datetime.utcnow)
    ist_minute = Column(DateTime, index=True, nullable=False)
    instrument = Column(String, index=True)
    expiry = Column(Date, index=True)
    underlying_price = Column(Float)

    total_net_gex = Column(Float)
    gamma_flip_level = Column(Float)
    otm_call_vega = Column(Float)
    otm_put_vega = Column(Float)
    otm_call_theta = Column(Float)
    otm_put_theta = Column(Float)
    otm_call_delta = Column(Float)
    otm_put_delta = Column(Float)

    __table_args__ = (
        Index("ix_hist_summary_minute_instrument", "ist_minute", "instrument"),
    )
