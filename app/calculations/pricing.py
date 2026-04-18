import pandas as pd


def pricing_range(comp_df: pd.DataFrame) -> dict:
    comp_rates = comp_df["comps"].explode()
    comp_median = comp_rates.median()

    your_avg_rate = comp_df["your_rate"].mean()
    adr_index = (your_avg_rate / comp_median) * 100

    lower = comp_median * 0.95
    upper = comp_median * 1.05

    return {
        "comp_median": float(round(comp_median, 2)),
        "your_index": float(round(adr_index, 1)),
        "recommended_range": [
            float(round(lower, 2)),
            float(round(upper, 2))
        ]
    }