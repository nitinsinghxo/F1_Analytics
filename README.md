Formula 1 Analytics: Ground Effect Era (2022–2025)

Overview

This project is a comprehensive Formula 1 analytics system built to analyze and simulate races from the ground-effect era (2022–2025).
It integrates qualifying, race, and season-level data to generate insights on driver performance, team efficiency, and race dynamics.
The project also prepares structured data for future dashboards in Tableau and Power BI.

Objectives

Analyze driver and team performance across multiple seasons (2022–2025).
Track position gains/losses from qualifying to race completion.
Model and simulate race outcomes under varying conditions.
Store and manage data in PostgreSQL for scalable analytics.
Build foundations for advanced predictive models and strategy simulations.
Enable interactive visual dashboards for non-technical users.

Project Structure
formula1/
│
├── data/                    # Raw and processed data
│   ├── f1data.db            # Local SQLite backup
│   └── exports/             # Cleaned CSV exports
│
├── notebooks/               # Exploratory & simulation notebooks
│   ├── 01_data_cleaning.ipynb
│   ├── 02_eda.ipynb
│   ├── 03_simulation.ipynb
│
├── src/                     # Core Python scripts
│   ├── config.py            # Environment and DB config
│   ├── db.py                # PostgreSQL connection logic
│   ├── etl.py               # Data loading & transformation
│
├── .gitignore               # Ignored files and directories
├── requirements.txt         # Dependencies
└── README.md                # Project documentation

Key Analytical Features

Qualifying Analysis – Derives driver starting order, team performance, and grid consistency.
Race Analysis – Tracks lap completions, DNFs, and finishing positions.
Position Delta Computation – Quantifies driver gains/losses from qualifying to race.
Performance Metrics – Calculates average points, team efficiency, and reliability trends.

Insights Generated

Average driver and team points over the 2022–2025 seasons.
Distribution of grid position vs final position.
Consistency metrics for qualifying and race results.
Position delta-based ranking of aggressive vs consistent drivers.
Baseline models for race outcome prediction and comparison.

Future Extensions

Integration with Tableau and Power BI dashboards.
Real-time data ingestion for live race analytics.
Enhanced race simulation models using historical strategy data.
Driver and team performance forecasting beyond 2025.

Notes

This project focuses exclusively on the 2022–2025 Formula 1 ground-effect era, a pivotal phase in F1 history marked by aerodynamic regulation changes, evolving car philosophies, and shifting competitive balances.
The aim is to quantify this era’s dynamics before the next regulatory shift in 2026.

