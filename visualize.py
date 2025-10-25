import pandas as pd
import plotly.express as px
from sqlalchemy import text
from src.db import engine


def load_simulation_frames(simulation_id: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    laps = pd.read_sql(text("SELECT * FROM simulation_laps WHERE simulation_id = :sid ORDER BY lap, position"), engine, params={"sid": simulation_id})
    results = pd.read_sql(text("SELECT * FROM simulation_results WHERE simulation_id = :sid ORDER BY finish_position"), engine, params={"sid": simulation_id})
    return laps, results


def fig_positions_over_laps(simulation_id: int):
    laps, _ = load_simulation_frames(simulation_id)
    if laps.empty:
        return None
    fig = px.line(
        laps,
        x="lap",
        y="position",
        color="driver",
        line_group="driver",
        title=f"Positions over Laps (Sim {simulation_id})",
        markers=False,
    )
    fig.update_yaxes(autorange="reversed", dtick=1)
    fig.update_layout(legend_title_text="Driver")
    return fig


def fig_stint_tyre_heatmap(simulation_id: int):
    laps, _ = load_simulation_frames(simulation_id)
    if laps.empty:
        return None
    pivot = laps.pivot_table(index="driver", columns="lap", values="tyre_compound", aggfunc="last")
    # Encode compounds ordinally for coloring
    mapping = {"Soft": 2, "Medium": 1, "Hard": 0}
    encoded = pivot.applymap(lambda x: mapping.get(x, -1))
    fig = px.imshow(
        encoded,
        color_continuous_scale=["#999", "#FDBA74", "#22C55E"],
        origin="lower",
        aspect="auto",
        labels=dict(color="Compound (H/M/S)"),
        title=f"Tyre Compounds by Lap (Sim {simulation_id})",
    )
    fig.update_yaxes(ticktext=list(pivot.index), tickvals=list(range(len(pivot.index))))
    return fig


def fig_finish_bar(simulation_id: int):
    _, results = load_simulation_frames(simulation_id)
    if results.empty:
        return None
    fig = px.bar(
        results,
        x="driver",
        y="points",
        color="finish_position",
        title=f"Simulated Points by Driver (Sim {simulation_id})",
    )
    return fig



