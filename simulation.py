import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Optional
from pathlib import Path

from sqlalchemy import insert

from src.db import engine, simulations, simulation_results, simulation_laps, simulation_pitstops


@dataclass
class StrategyPlan:
    driver: str
    planned_pit_laps: List[int]
    starting_compound: str


def _load_event_data(season: int, round_number: int, data_dir: str = "data") -> Dict[str, pd.DataFrame]:
    data_path = Path(data_dir)
    races = pd.read_csv(data_path / "races_2022_2025.csv")
    results = pd.read_csv(data_path / "results_2022_2025.csv")
    quali = pd.read_csv(data_path / "qualifying_2022_2025.csv")
    laps = pd.read_csv(data_path / "laps_2022_2025.csv")

    races_evt = races[(races["season"] == season) & (races["round"] == round_number)]
    event_name = races_evt.iloc[0]["race_name"] if not races_evt.empty else f"Round {round_number}"

    res_evt = results[(results["season"] == season) & (results["round"] == round_number)]
    quali_evt = quali[(quali["season"] == season) & (quali["round"] == round_number)]
    laps_evt = laps[(laps["season"] == season) & (laps["round"] == round_number)]

    # Try to infer grid from quali, fallback to results.grid
    grid = quali_evt[["driver", "position", "team"]].rename(columns={"position": "grid_position"})
    if grid["grid_position"].isna().all():
        grid = res_evt[["driver", "grid", "team"]].rename(columns={"grid": "grid_position"})

    grid = grid.dropna(subset=["grid_position"]).copy()
    grid["grid_position"] = grid["grid_position"].astype(int)

    return {
        "event_name": event_name,
        "grid": grid,
        "results": res_evt,
        "laps": laps_evt,
    }


def _estimate_base_pace(laps_evt: pd.DataFrame) -> Dict[str, float]:
    if laps_evt.empty:
        return {}
    # Use median clean lap times per driver as base pace
    cols = [c for c in laps_evt.columns]
    time_col = None
    for cand in ["LapTime", "LapTimeSeconds", "LapTime_s", "Time"]:
        if cand in cols:
            time_col = cand
            break
    if time_col is None:
        # try to construct lap time if FastF1 formatting
        if {"LapTime", "Driver"}.issubset(set(cols)):
            # FastF1 laps often have Timedelta-like strings; let pandas parse
            lap_times = (
                laps_evt[["Driver", "LapTime"]]
                .dropna()
                .assign(LapTimeSeconds=lambda d: pd.to_timedelta(d["LapTime"]).dt.total_seconds())
            )
            base = lap_times.groupby("Driver")["LapTimeSeconds"].median().to_dict()
            return base
        return {}

    driver_col = "Driver" if "Driver" in laps_evt.columns else ("driver" if "driver" in laps_evt.columns else None)
    if driver_col is None:
        return {}

    df = laps_evt[[driver_col, time_col]].dropna()
    if np.issubdtype(df[time_col].dtype, np.number):
        base = df.groupby(driver_col)[time_col].median().to_dict()
    else:
        base = (
            df.assign(_s=lambda d: pd.to_timedelta(d[time_col]).dt.total_seconds())
            .groupby(driver_col)["_s"].median()
            .to_dict()
        )
    return base


def _default_strategies(grid_df: pd.DataFrame, race_laps: int) -> Dict[str, StrategyPlan]:
    strategies: Dict[str, StrategyPlan] = {}
    for _, row in grid_df.sort_values("grid_position").iterrows():
        driver = row["driver"] if "driver" in row else row["Driver"]
        # simple 1 or 2-stop heuristic by grid position
        if row["grid_position"] <= 5 and race_laps >= 50:
            planned = [18, 40]
        elif race_laps >= 40:
            planned = [22]
        else:
            planned = [int(race_laps * 0.55)]
        strategies[driver] = StrategyPlan(driver=driver, planned_pit_laps=planned, starting_compound="Medium")
    return strategies


def simulate_race(
    season: int,
    round_number: int,
    strategy_overrides: Optional[Dict[str, StrategyPlan]] = None,
    random_seed: int = 42,
    data_dir: str = "data",
) -> int:
    rng = np.random.default_rng(random_seed)
    evt = _load_event_data(season, round_number, data_dir=data_dir)
    event_name = evt["event_name"]
    grid_df = evt["grid"].copy()
    laps_evt = evt["laps"].copy()

    # Determine race distance from historical laps
    if not laps_evt.empty and "LapNumber" in laps_evt.columns:
        race_laps = int(laps_evt["LapNumber"].max())
    else:
        # fallback typical distance
        race_laps = 57

    base_pace = _estimate_base_pace(laps_evt)
    drivers = list(grid_df.sort_values("grid_position")["driver"].values)

    strategies = _default_strategies(grid_df, race_laps)
    if strategy_overrides:
        strategies.update(strategy_overrides)

    # Insert simulation record
    with engine.begin() as conn:
        res = conn.execute(
            insert(simulations).values(
                season=season,
                round=round_number,
                event_name=event_name,
                strategy_model="heuristic_v1",
            )
        )
        sim_id = res.inserted_primary_key[0]

    # Initialize state
    driver_state = {}
    for _, row in grid_df.iterrows():
        d = row["driver"]
        driver_state[d] = {
            "position": int(row["grid_position"]),
            "stint": 1,
            "compound": strategies[d].starting_compound if d in strategies else "Medium",
            "total_time": 0.0,
            "pitted_laps": set(),
        }

    lap_records = []
    pit_records = []

    for lap in range(1, race_laps + 1):
        lap_times = {}
        for d in drivers:
            base = base_pace.get(d, 90.0)
            wear_penalty = 0.08 * (lap % 15)
            traffic = 0.02 * (driver_state[d]["position"] - 1)
            randomness = rng.normal(0, 0.15)

            pit_time = 0.0
            is_pit = False
            plan = strategies.get(d)
            if plan and lap in plan.planned_pit_laps and lap not in driver_state[d]["pitted_laps"]:
                is_pit = True
                pit_time = 22.0 + rng.normal(0, 0.8)
                driver_state[d]["pitted_laps"].add(lap)
                driver_state[d]["stint"] += 1
                # simple compound switch
                driver_state[d]["compound"] = "Hard" if driver_state[d]["compound"] != "Hard" else "Medium"
                pit_records.append({
                    "simulation_id": sim_id,
                    "driver": d,
                    "lap": lap,
                    "pit_time_s": max(18.0, pit_time),
                    "from_compound": "Hard" if driver_state[d]["compound"] == "Hard" else "Medium",
                    "to_compound": driver_state[d]["compound"],
                })

            lap_time = base + wear_penalty + traffic + randomness + pit_time
            lap_time = float(max(75.0, lap_time))
            lap_times[d] = lap_time

        # Recompute positions based on cumulative time
        for d in drivers:
            driver_state[d]["total_time"] += lap_times[d]

        ranking = sorted(drivers, key=lambda x: driver_state[x]["total_time"])  # smaller time -> ahead
        for pos, d in enumerate(ranking, start=1):
            driver_state[d]["position"] = pos

        for d in drivers:
            lap_records.append({
                "simulation_id": sim_id,
                "lap": lap,
                "driver": d,
                "position": driver_state[d]["position"],
                "lap_time_s": lap_times[d],
                "stint": driver_state[d]["stint"],
                "tyre_compound": driver_state[d]["compound"],
                "is_pit": lap in driver_state[d]["pitted_laps"],
            })

    # Final results
    final_order = sorted(drivers, key=lambda x: driver_state[x]["total_time"])  # winner first
    results_rows = []
    for finish_pos, d in enumerate(final_order, start=1):
        team_val = None
        if "team" in grid_df.columns:
            team_val = grid_df.loc[grid_df["driver"] == d, "team"].iloc[0]
        grid_pos = int(grid_df.loc[grid_df["driver"] == d, "grid_position"].iloc[0])
        results_rows.append({
            "simulation_id": sim_id,
            "driver": d,
            "team": team_val,
            "grid_position": grid_pos,
            "finish_position": finish_pos,
            "points": _points_for_pos(finish_pos),
            "status": "Finished",
            "total_time_s": float(driver_state[d]["total_time"]),
        })

    # Write to DB
    with engine.begin() as conn:
        if lap_records:
            conn.execute(insert(simulation_laps), lap_records)
        if pit_records:
            conn.execute(insert(simulation_pitstops), pit_records)
        if results_rows:
            conn.execute(insert(simulation_results), results_rows)

    return sim_id


def _points_for_pos(pos: int) -> float:
    mapping = {1: 25, 2: 18, 3: 15, 4: 12, 5: 10, 6: 8, 7: 6, 8: 4, 9: 2, 10: 1}
    return float(mapping.get(pos, 0))


