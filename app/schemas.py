from pydantic import BaseModel
from typing import List
from datetime import date


class OwnerCreate(BaseModel):
    owner_id: str
    owner_name: str
    email: str
    service_tier: str


class HotelCreate(BaseModel):
    hotel_id: str
    owner_id: str
    hotel_name: str
    rooms_available: int
    currency_code: str
    currency_symbol: str


class DailyPerformance(BaseModel):
    date: date
    rooms_sold: int
    room_revenue: float


class CompSetRate(BaseModel):
    date: date
    your_rate: float
    comps: List[float]
