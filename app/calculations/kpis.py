import pandas as pd


def calculate_kpis(df: pd.DataFrame, rooms_available: int) -> dict:
    df = df.copy()

    df["occupancy"] = df["rooms_sold"] / rooms_available
    df["adr"] = df["room_revenue"] / df["rooms_sold"]
    df["revpar"] = df["room_revenue"] / rooms_available

    return {
        "occupancy": float(round(df["occupancy"].mean() * 100, 2)),
        "adr": float(round(df["adr"].mean(), 2)),
        "revpar": float(round(df["revpar"].mean(), 2)),
        "total_room_revenue": float(round(df["room_revenue"].sum(), 2)),
    }