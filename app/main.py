from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import os

# ─────────────────────────────────────────────
# FastAPI App Configuration
# ─────────────────────────────────────────────
app = FastAPI()

# CORS - Allow all for testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Key security (simplified for now)
API_KEY_HEADER = APIKeyHeader(name="X-Owner-Token")

# Simple token check (replace with your actual token)
VALID_TOKENS = ["9MfYQDx1lVGWFFiQ_D9ibK7lMnruUU6-1jDqapC2if4"]

async def verify_token(api_key: str = Security(API_KEY_HEADER)):
    if api_key not in VALID_TOKENS:
        raise HTTPException(status_code=403, detail="Invalid token")
    return api_key

# ─────────────────────────────────────────────
# Request/Response Models
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

# ─────────────────────────────────────────────
# Test Endpoint
# ─────────────────────────────────────────────
@app.get("/")
async def root():
    return {"message": "Backend is running", "status": "ok"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

# ─────────────────────────────────────────────
# Rate Intelligence Endpoint
# ─────────────────────────────────────────────
@app.post("/api/rate-intelligence")
async def get_rate_intelligence(
    request: RateIntelligenceRequest,
    token: str = Depends(verify_token)
):
    # Start with current rate
    suggested_rate = request.current_rate
    
    # Adjust based on demand
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
    
    # Adjust based on competitors
    if request.competitor_rates and len(request.competitor_rates) > 0:
        comp_avg = sum(request.competitor_rates) / len(request.competitor_rates)
        if comp_avg > request.current_rate * 1.1:
            suggested_rate = max(suggested_rate, request.current_rate * 1.05)
            recommendation = "Below competitors - increase rate"
        elif comp_avg < request.current_rate * 0.9:
            suggested_rate = min(suggested_rate, request.current_rate * 0.97)
            recommendation = "Above competitors - slight decrease"
    
    # Round to nearest 10
    suggested_rate = round(suggested_rate / 10) * 10
    
    # Calculate confidence
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

# ─────────────────────────────────────────────
# Mock endpoints for testing (bypass database)
# ─────────────────────────────────────────────
@app.get("/hotel_dashboard_history/{hotel_id}")
async def get_hotel_dashboard_history(
    hotel_id: str,
    token: str = Depends(verify_token)
):
    # Return mock data for testing
    return [
        {
            "snapshot_id": 1,
            "hotel_id": hotel_id,
            "period_start": "2026-04-01",
            "period_end": "2026-04-30",
            "created_at": "2026-04-22T00:00:00"
        }
    ]

@app.get("/daily_by_snapshot/{snapshot_id}")
async def get_daily_by_snapshot(
    snapshot_id: int,
    token: str = Depends(verify_token)
):
    # Return mock data for testing
    return {
        "performance": [
            {"stay_date": "2026-04-01", "rooms_sold": 18, "room_revenue": 16200, "adr": 900},
            {"stay_date": "2026-04-02", "rooms_sold": 20, "room_revenue": 19000, "adr": 950},
            {"stay_date": "2026-04-03", "rooms_sold": 17, "room_revenue": 16150, "adr": 950},
        ],
        "compset": [
            {"stay_date": "2026-04-01", "your_rate": 900, "comps": "[920, 880, 950]"},
            {"stay_date": "2026-04-02", "your_rate": 950, "comps": "[980, 960, 940]"},
            {"stay_date": "2026-04-03", "your_rate": 950, "comps": "[970, 930, 960]"},
        ]
    }
