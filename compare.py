import pandas as pd
from sqlalchemy import insert
from src.db import engine, simulation_results, simulation_vs_actual


def compare_sim_to_actual(simulation_id: int, season: int, round_number: int, data_dir: str = "data") -> pd.DataFrame:
    """
    Compare a simulation to actual results and persist per-driver diffs.
    """
    # Load actual results from CSV store
    actual = pd.read_csv(f"{data_dir}/results_2022_2025.csv")
    actual_evt = actual[(actual["season"] == season) & (actual["round"] == round_number)]
    actual_evt = actual_evt[["driver", "position", "points"]].rename(
        columns={"position": "actual_finish", "points": "actual_points"}
    )

    # Load sim results from DB
    sim_df = pd.read_sql_table("simulation_results", con=engine)
    sim_evt = sim_df[sim_df["simulation_id"] == simulation_id][["driver", "finish_position", "points"]].rename(
        columns={"finish_position": "sim_finish", "points": "sim_points"}
    )

    merged = actual_evt.merge(sim_evt, on="driver", how="inner")
    merged["diff_positions"] = merged["sim_finish"] - merged["actual_finish"]

    # Persist
    records = merged.assign(season=season, round=round_number)[
        ["season", "round", "driver", "actual_finish", "sim_finish", "diff_positions", "actual_points", "sim_points"]
    ].to_dict(orient="records")
    if records:
        with engine.begin() as conn:
            conn.execute(insert(simulation_vs_actual), records)
    return merged



