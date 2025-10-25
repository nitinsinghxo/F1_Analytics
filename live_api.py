from typing import Dict, Tuple
import pandas as pd
import fastf1


def get_event_grid_and_results(season: int, round_number: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retrieve grid (from qualifying or race grid) and actual race results
    for an event using FastF1. Suitable for near-live (polling) if telemetry
    and cache are enabled externally.
    """
    # Qualifying
    try:
        q = fastf1.get_session(season, round_number, "Q")
        q.load(results=True, laps=False, telemetry=False)
        qdf = q.results.reset_index()
        grid = qdf[["Abbreviation", "TeamName", "Position"]].rename(
            columns={"Abbreviation": "driver", "TeamName": "team", "Position": "grid_position"}
        )
    except Exception:
        grid = pd.DataFrame(columns=["driver", "team", "grid_position"]).copy()

    # Race results (if completed / during)
    results = pd.DataFrame()
    try:
        r = fastf1.get_session(season, round_number, "R")
        r.load(results=True, laps=False, telemetry=False)
        rdf = r.results.reset_index()
        results = rdf[["Abbreviation", "TeamName", "Position", "Points", "Time"]].rename(
            columns={
                "Abbreviation": "driver",
                "TeamName": "team",
                "Position": "position",
                "Points": "points",
                "Time": "time",
            }
        )
    except Exception:
        pass

    return grid, results


def get_event_laps(season: int, round_number: int) -> pd.DataFrame:
    """Retrieve laps dataframe for an event (can be used in polling)."""
    try:
        r = fastf1.get_session(season, round_number, "R")
        r.load(laps=True, telemetry=False)
        laps = r.laps.copy()
        # Normalize driver column name
        if "Driver" in laps.columns:
            laps = laps.rename(columns={"Driver": "driver"})
        return laps
    except Exception:
        return pd.DataFrame()



