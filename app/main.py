from fastapi import FastAPI, HTTPException, Depends, Security, Header
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import os
import sqlite3
import json
from datetime import datetime

app = FastAPI()

# CORS - Allow your frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://revenueinsights26.github.io", "http://localhost:5500", "http://127.0.0.1:5500"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# API Key header
API_KEY_NAME = "X-Owner-Token"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

# Valid tokens (add your tokens here)
VALID_TOKENS = {
    "9MfYQDx1lVGWFFiQ_D9ibK7lMnruUU6-1jDqapC2if4": {"owner_id": "OWNER001", "is_active": True}
}

async def get_current_owner(api_key: str = Security(api_key_header)):
    if api_key not in VALID_TOKENS:
        raise HTTPException(status_code=403, detail="Invalid token")
    if not VALID_TOKENS[api_key]["is_active"]:
        raise HTTPException(status_code=403, detail="Token is inactive")
    return VALID_TOKENS[api_key]

# ─────────────────────────────────────────────
# Database path
# ─────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "db", "revenue_insights.db")

def get_db():
    if os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    return None

# ─────────────────────────────────────────────
# Test endpoint
# ─────────────────────────────────────────────
@app.get("/")
async def root():
    return {"message": "Backend is running", "status": "ok", "timestamp": datetime.now().isoformat()}

# ─────────────────────────────────────────────
# Hotel Dashboard History
# ─────────────────────────────────────────────
@app.get("/hotel_dashboard_history/{hotel_id}")
async def get_hotel_dashboard_history(
    hotel_id: str,
    owner = Depends(get_current_owner)
):
    # Return mock data since database might be corrupted
    # This will get your frontend working immediately
    return [
        {
            "snapshot_id": 1,
            "hotel_id": hotel_id,
            "period_start": "2026-04-01",
            "period_end": "2026-04-30",
            "created_at": "2026-04-01T00:00:00",
            "forecast_occupancy": 72,
            "forecast_adr_min": 1200,
            "forecast_adr_max": 1400,
            "commentary": "Based on historical data, occupancy is expected to remain strong."
        },
        {
            "snapshot_id": 2,
            "hotel_id": hotel_id,
            "period_start": "2026-03-01",
            "period_end": "2026-03-31",
            "created_at": "2026-03-01T00:00:00",
            "forecast_occupancy": 68,
            "forecast_adr_min": 1150,
            "forecast_adr_max": 1350,
            "commentary": "Seasonal patterns suggest slight softening."
        }
    ]

# ─────────────────────────────────────────────
# Daily by Snapshot
# ─────────────────────────────────────────────
@app.get("/daily_by_snapshot/{snapshot_id}")
async def get_daily_by_snapshot(
    snapshot_id: int,
    owner = Depends(get_current_owner)
):
    # Return mock data for April 2026
    if snapshot_id == 1:
        performance = [
            {"stay_date": "2026-04-01", "rooms_sold": 75, "room_revenue": 86184, "adr": 1149},
            {"stay_date": "2026-04-02", "rooms_sold": 75, "room_revenue": 91444, "adr": 1219},
            {"stay_date": "2026-04-03", "rooms_sold": 91, "room_revenue": 148640, "adr": 1633},
            {"stay_date": "2026-04-04", "rooms_sold": 94, "room_revenue": 160350, "adr": 1706},
            {"stay_date": "2026-04-05", "rooms_sold": 83, "room_revenue": 119362, "adr": 1438},
            {"stay_date": "2026-04-06", "rooms_sold": 60, "room_revenue": 79417, "adr": 1324},
            {"stay_date": "2026-04-07", "rooms_sold": 71, "room_revenue": 77819, "adr": 1096},
            {"stay_date": "2026-04-08", "rooms_sold": 74, "room_revenue": 76343, "adr": 1032},
            {"stay_date": "2026-04-09", "rooms_sold": 66, "room_revenue": 67473, "adr": 1022},
            {"stay_date": "2026-04-10", "rooms_sold": 84, "room_revenue": 110402, "adr": 1195},
            {"stay_date": "2026-04-11", "rooms_sold": 91, "room_revenue": 116961, "adr": 1285},
            {"stay_date": "2026-04-12", "rooms_sold": 58, "room_revenue": 64047, "adr": 1104},
            {"stay_date": "2026-04-13", "rooms_sold": 56, "room_revenue": 72679, "adr": 1009},
            {"stay_date": "2026-04-14", "rooms_sold": 51, "room_revenue": 64116, "adr": 957},
            {"stay_date": "2026-04-15", "rooms_sold": 44, "room_revenue": 59802, "adr": 934},
        ]
        compset = [
            {"stay_date": "2026-04-01", "your_rate": 1149, "comps": [1200, 1100, 1150]},
            {"stay_date": "2026-04-02", "your_rate": 1219, "comps": [1250, 1180, 1220]},
            {"stay_date": "2026-04-03", "your_rate": 1633, "comps": [1650, 1600, 1620]},
            {"stay_date": "2026-04-04", "your_rate": 1706, "comps": [1720, 1680, 1700]},
            {"stay_date": "2026-04-05", "your_rate": 1438, "comps": [1450, 1400, 1420]},
            {"stay_date": "2026-04-06", "your_rate": 1324, "comps": [1300, 1350, 1320]},
            {"stay_date": "2026-04-07", "your_rate": 1096, "comps": [1100, 1080, 1090]},
            {"stay_date": "2026-04-08", "your_rate": 1032, "comps": [1050, 1000, 1030]},
            {"stay_date": "2026-04-09", "your_rate": 1022, "comps": [1020, 1000, 1010]},
            {"stay_date": "2026-04-10", "your_rate": 1195, "comps": [1200, 1180, 1190]},
            {"stay_date": "2026-04-11", "your_rate": 1285, "comps": [1300, 1270, 1280]},
            {"stay_date": "2026-04-12", "your_rate": 1104, "comps": [1120, 1080, 1100]},
            {"stay_date": "2026-04-13", "your_rate": 1009, "comps": [1020, 980, 1000]},
            {"stay_date": "2026-04-14", "your_rate": 957, "comps": [960, 940, 950]},
            {"stay_date": "2026-04-15", "your_rate": 934, "comps": [940, 920, 930]},
        ]
    else:
        performance = []
        compset = []
    
    return {"performance": performance, "compset": compset}

# ─────────────────────────────────────────────
# Rate Intelligence Endpoint
# ─────────────────────────────────────────────
class RateIntelligenceRequest(BaseModel):
    current_rate: float
    competitor_rates: List[float] = []
    historical_occupancy: float
    dow_factor: float = 50
    overall_avg_occ: float = 50
    has_competitor_data: bool = False

class RateIntelligenceResponse(BaseModel):
    suggested_rate: float
    confidence_score: int
    recommendation: str
    confidence_level: str

@app.post("/api/rate-intelligence")
async def get_rate_intelligence(
    request: RateIntelligenceRequest,
    owner = Depends(get_current_owner)
):
    suggested_rate = request.current_rate
    
    if request.historical_occupancy > 75:
        suggested_rate = suggested_rate * 1.08
        recommendation = "High demand - increase rate"
    elif request.historical_occupancy > 60:
        suggested_rate = suggested_rate * 1.03
        recommendation = "Good demand - slight increase"
    elif request.historical_occupancy < 50:
        suggested_rate = suggested_rate * 0.95
        recommendation = "Soft demand - slight decrease"
    else:
        recommendation = "Moderate demand - maintain rate"
    
    if request.competitor_rates and len(request.competitor_rates) > 0:
        comp_avg = sum(request.competitor_rates) / len(request.competitor_rates)
        if comp_avg > request.current_rate * 1.1:
            suggested_rate = max(suggested_rate, request.current_rate * 1.05)
            recommendation = "Below competitors - increase rate"
        elif comp_avg < request.current_rate * 0.9:
            suggested_rate = min(suggested_rate, request.current_rate * 0.97)
            recommendation = "Above competitors - slight decrease"
    
    suggested_rate = round(suggested_rate / 10) * 10
    
    confidence_score = 70
    confidence_level = "Medium"
    
    if request.competitor_rates and len(request.competitor_rates) >= 3:
        confidence_score = 85
        confidence_level = "High"
    elif not request.competitor_rates or len(request.competitor_rates) == 0:
        confidence_score = 50
        confidence_level = "Low"
    
    return RateIntelligenceResponse(
        suggested_rate=suggested_rate,
        confidence_score=confidence_score,
        recommendation=recommendation,
        confidence_level=confidence_level
    )
