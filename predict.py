import pandas as pd
import numpy as np
from typing import Dict


def predict_finishing_positions(season: int, round_number: int, data_dir: str = "data") -> pd.DataFrame:
    """
    Simple baseline predictor using qualifying position and team average points.
    Outputs a dataframe with predicted order.
    """
    results = pd.read_csv(f"{data_dir}/results_2022_2025.csv")
    quali = pd.read_csv(f"{data_dir}/qualifying_2022_2025.csv")

    # Team strength from rolling avg points (last 8 races prior to event)
    evt_mask = (results["season"] < season) | ((results["season"] == season) & (results["round"] < round_number))
    hist = results[evt_mask].copy()
    team_points = hist.groupby(["team"]).agg(team_avg_points=("points", "mean")).reset_index()

    grid = quali[(quali["season"] == season) & (quali["round"] == round_number)][["driver", "team", "position"]].rename(
        columns={"position": "grid_position"}
    )

    df = grid.merge(team_points, on="team", how="left").fillna({"team_avg_points": 0.0})

    # Score = -grid_position + alpha * team_strength
    alpha = 0.25
    df["score"] = -df["grid_position"].astype(float) + alpha * df["team_avg_points"].astype(float)
    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    df["predicted_finish"] = np.arange(1, len(df) + 1)
    return df[["driver", "team", "grid_position", "predicted_finish", "score"]]



