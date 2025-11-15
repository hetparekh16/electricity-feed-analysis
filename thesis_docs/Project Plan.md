Questions to ask: 

1. Data access for all datas, how and when?
2. Git repository creation?
3. Thesis report template and guidelines?
4. Update meetings?
5. Deliverables?

## Phase 1 — Setup & Literature Review (Weeks 1–3)

**Goal:** Build context, finalize requirements, prepare the data environment.

**Tasks:**

1. **Review background literature:**

    - Data fusion for renewable forecasting.
    - Wind & PV modeling from weather data.
    - Storage siting & sizing in grid studies.

2. **Clarify data availability:**

    - Confirm access to **WeDar**, **MaStr**, **SMARD/ENTSO-E**.
    - Check for **HV grid** model (PyPSA-DE or SciGRID).

3. **Define grid & region boundaries:** Quadrants vs. TSO zones.
    
4. **Prototype environment:**

    - Create project repo (Git).
    - Set up DB (DuckDB + GeoPandas).
    - Create notebook environment (uv).

5. **Deliverable:** Short “status quo” write-up (per the brief) + architecture draft.

---

## Phase 2 — Data Ingestion & Harmonization (Weeks 3–6)

**Goal:** Build the data-fusion backbone.

**Tasks:**

1. **Ingestion layer:**

    - Load & standardize weather (WeDar), assets (MaStr), and actuals (ENTSO-E/SMARD).
    - Define schemas (time, space, units, variable names).

2. **Spatial harmonization:**

    - Project all data to **ETRS89 / UTM 32N**, 2×2 km grid.
    - Snap MaStr points into grid cells.

3. **Temporal harmonization:**

    - Convert all timestamps to UTC, hourly resolution.

4. **Quality checks:**

    - Handle missing data, remove anomalies, flag inactive assets.

5. **Deliverable:** Functional **ETL pipeline** + harmonized sample dataset.

---

## Phase 3 — Forecasting Model (Task 1, Weeks 6–10)

**Goal:** Produce 0–48 h renewable infeed forecasts with uncertainty.

**Tasks:**

1. **Feature engineering:** Hub-height wind speed, irradiance → POA, air density.
2. **Modeling:**

    - Wind: turbine power curves, aggregation per cell.
    - PV: irradiance → DC → AC (performance ratio, clipping).

3. **Uncertainty modeling:** Use ensemble forecasts or quantile regression to get P10/P50/P90.

4. **Validation:**

    - Aggregate to TSO/country and compare with SMARD.
    - Compute MAE/RMSE and CRPS/coverage.

5. **Deliverable:** Forecast dataset (48 h), model notebook, evaluation plots.

---

## Phase 4 — Hindcast & Spatial Distribution (Task 2, Weeks 10–12)

**Goal:** Build annual/seasonal maps of renewable infeed and capacity factors.

**Tasks:**

1. Run Task 1 model on historical weather (e.g., 2024).
2. Compute per-cell MWh and capacity factors.
3. Aggregate by quadrants and TSOs.
4. Create heatmaps, time-aggregated summaries.
5. **Deliverable:** Spatial maps (GeoTIFF/GeoJSON) and quadrant summaries.

---

## Phase 5 — Inter-Area Power Flows (Task 3, Weeks 12–14)

**Goal:** Simulate inter-area transfers under selected weather scenarios.

**Tasks:**

1. Link cell-level injections to HV grid nodes.
2. Build DC power-flow model (PyPSA-DE, SciGRID, or simplified network).
3. Define weather scenarios (high-wind, low-wind, high-solar, typical).
4. Compute inter-area flows and congestion frequency.
5. **Deliverable:** Flow maps, time-series plots, congestion reports.

---

## Phase 6 — Storage Location & Sizing (Task 4, Weeks 14–16)

**Goal:** Identify top candidate sites and size storage power rating.

**Tasks:**

1. Rank nodes by congestion frequency × severity.
2. Estimate MW needed to cap typical overflow (energy optional).
3. Verify improvements by re-running flow with storage dispatch.
4. **Deliverable:** Ranked list of storage locations + MW ratings + improvement metrics.

---

## Phase 7 — Analysis Tool, GUI, and Documentation (Weeks 16–18)

**Goal:** Deliver a small end-user tool and reproducibility package.

**Tasks:**

1. Develop GUI (map + time slider + plots + export).

    - Framework: **Dash**, **Streamlit**, or **Panel**.

2. Link GUI to DB or local data API.
3. Write documentation (data dictionary, setup, usage).
4. **Deliverable:** Working tool, documented codebase.

---

## Phase 8 — Results, Discussion & Presentation (Weeks 18–20)

**Goal:** Wrap-up, visualize, and present findings.

**Tasks:**

1. Summarize results for all tasks.
2. Discuss accuracy, limitations, and implications.
3. Prepare final report & presentation slides.
4. Practice final defense with supervisor feedback.