def simple_forecast(current_occupancy: float, current_adr: float) -> dict:
    return {
        "forecast_occupancy": float(round(current_occupancy, 1)),
        "forecast_adr_range": [
            float(round(current_adr * 0.97, 0)),
            float(round(current_adr * 1.03, 0))
        ]
    }