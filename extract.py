# src/extract.py
import fastf1
import pandas as pd
from pathlib import Path
from tqdm import tqdm

def setup_cache_and_dirs(cache_dir="fastf1_cache", data_dir="data"):
    Path(cache_dir).mkdir(exist_ok=True)
    Path(data_dir).mkdir(exist_ok=True)
    fastf1.Cache.enable_cache(cache_dir)
    return Path(data_dir)

def fetch_f1_data(years=range(2022, 2026), data_dir="data"):
    """Fetch races, results, qualifying, laps, drivers, constructors, circuits."""
    data_dir = Path(data_dir)
    races_all, results_all, qualifying_all, laps_all = [], [], [], []
    drivers_map, constructors_map, circuits_map = {}, {}, {}

    for year in tqdm(years, desc="Seasons"):
        try:
            schedule = fastf1.get_event_schedule(year)
        except Exception as e:
            print(f"Skipping year {year} - could not load schedule: {e}")
            continue

        for _, race in schedule.iterrows():
            round_ = race['RoundNumber']
            race_name = race['EventName']
            circuit = race['Location']
            date = race['EventDate']

            if pd.to_datetime(date) > pd.Timestamp.now():
                continue

            races_all.append({
                "season": year,
                "round": round_,
                "race_name": race_name,
                "circuit": circuit,
                "date": date
            })
            circuits_map[circuit] = {"name": circuit, "location": race['Location'], "country": race['Country']}

            # Load race session
            try:
                race_session = fastf1.get_session(year, round_, 'R')
                race_session.load(laps=True, telemetry=True)
            except Exception as e:
                print(f"Skipping {race_name} ({year}) - race data unavailable: {e}")
                continue

            # Results
            res_df = race_session.results.reset_index()
            for _, r in res_df.iterrows():
                results_all.append({
                    "season": year,
                    "round": round_,
                    "driver": r['Abbreviation'],
                    "team": r['TeamName'],
                    "position": r['Position'],
                    "laps": r['Laps'],
                    "time": r['Time'],
                    "points": r['Points'],
                    "fastest_lap": r.get('FastestLap', None),
                    "grid": r.get("Grid", None)
                })
                drivers_map[r['Abbreviation']] = {"driver": r['FullName'], "team": r['TeamName']}
                constructors_map[r['TeamName']] = {"team": r['TeamName']}

            # Qualifying
            try:
                qual_session = fastf1.get_session(year, round_, 'Q')
                qual_session.load(results=True, laps=False, telemetry=False)
                qual_df = qual_session.results.reset_index()
                for _, q in qual_df.iterrows():
                    qualifying_all.append({
                        "season": year,
                        "round": round_,
                        "driver": q['Abbreviation'],
                        "team": q['TeamName'] if 'TeamName' in q else q.get('Team', None),
                        "position": q['Position'],
                        "q1": q.get('Q1', None),
                        "q2": q.get('Q2', None),
                        "q3": q.get('Q3', None)
                    })
            except Exception:
                # fallback: use grid from race if quali missing
                for _, r in res_df.iterrows():
                    qualifying_all.append({
                        "season": year,
                        "round": round_,
                        "driver": r['Abbreviation'],
                        "team": r['TeamName'],
                        "position": r.get('Grid', None),
                        "q1": None,
                        "q2": None,
                        "q3": None
                    })
                print(f"Qualifying data missing for {race_name} ({year}) - using grid fallback")

            # Laps
            laps = race_session.laps.copy()
            laps['season'] = year
            laps['round'] = round_
            laps_all.append(laps)

    # Save CSVs
    pd.DataFrame(races_all).to_csv(data_dir/"races_2022_2025.csv", index=False)
    pd.DataFrame(results_all).to_csv(data_dir/"results_2022_2025.csv", index=False)
    pd.DataFrame(qualifying_all).to_csv(data_dir/"qualifying_2022_2025.csv", index=False)
    if laps_all:
        pd.concat(laps_all).to_csv(data_dir/"laps_2022_2025.csv", index=False)
    pd.DataFrame(list(drivers_map.values())).to_csv(data_dir/"drivers_2022_2025.csv", index=False)
    pd.DataFrame(list(constructors_map.values())).to_csv(data_dir/"constructors_2022_2025.csv", index=False)
    pd.DataFrame(list(circuits_map.values())).to_csv(data_dir/"circuits_2022_2025.csv", index=False)

    print("✅ All CSVs saved successfully.")
    return {
        "races": pd.DataFrame(races_all),
        "results": pd.DataFrame(results_all),
        "qualifying": pd.DataFrame(qualifying_all),
        "laps": pd.concat(laps_all) if laps_all else pd.DataFrame(),
        "drivers": pd.DataFrame(list(drivers_map.values())),
        "constructors": pd.DataFrame(list(constructors_map.values())),
        "circuits": pd.DataFrame(list(circuits_map.values()))
    }
