from pydantic import BaseModel, RootModel
from datetime import datetime, date
from typing import List, Dict, Optional

class GEXSummaryResponse(BaseModel):
    ist_minute: datetime
    underlying_price: Optional[float]
    total_net_gex: Optional[float]
    gamma_flip_level: Optional[float]
    total_call_oi: Optional[int]
    total_put_oi: Optional[int]


class StrikeGEXData(BaseModel):
    strike: float
    call_oi: Optional[int]
    put_oi: Optional[int]
    net_gex: Optional[float]
    abs_gex: Optional[float]


class TopStrikesResponse(BaseModel):
    top_abs_gex: List[StrikeGEXData]
    top_net_gex_pos: List[StrikeGEXData]
    top_net_gex_neg: List[StrikeGEXData]
    top_call_oi: List[StrikeGEXData]
    top_put_oi: List[StrikeGEXData]


class ChartDataPoint(BaseModel):
    ist_minute: datetime
    data: List[StrikeGEXData]


class ChartDataResponse(BaseModel):
    series: List[ChartDataPoint]


class OTMGreeksResponse(BaseModel):
    ist_minute: datetime
    otm_call_vega: Optional[float]
    otm_put_vega: Optional[float]
    otm_call_theta: Optional[float]
    otm_put_theta: Optional[float]
    otm_call_delta: Optional[float]
    otm_put_delta: Optional[float]


# class InstrumentExpiryMap(RootModel[Dict[str, List[date]]]):
#     pass


# class LatestMinuteResponse(BaseModel):
#     latest_minute: Optional[datetime]
