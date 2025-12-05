
## Overview


This directory contains historical weather forecast data from **Deutscher Wetterdienst (DWD)** using the **ICON-D2 model**. The data is organized by forecast initialization times and includes both **deterministic** and **ensemble probabilistic** forecasts for wind and solar radiation parameters relevant to electricity generation forecasting.


**Data Source:** DWD (Deutscher Wetterdienst - German Weather Service)
**Model:** ICON-D2 (Icosahedral Nonhydrostatic, Domain 2)
**Region:** Germany (de)
**Grid Type:** Latitude-Longitude regular grid
**File Format:** GRIB2 (`.grb2`)
**Update Frequency:** 3-hourly forecast runs (00, 03, 06, 09, 12, 15, 18, 21 UTC)

---
## Directory Structure

```
├── 2023050900
.
.
.
.
├── 2025110900/ # Forecast run: 2025-11-09 00:00 UTC
├── 2025110903/ # Forecast run: 2025-11-09 03:00 UTC
├── 2025110906/ # Forecast run: 2025-11-09 06:00 UTC
├── 2025110909/ # Forecast run: 2025-11-09 09:00 UTC
├── 2025110912/ # Forecast run: 2025-11-09 12:00 UTC
├── 2025110915/ # Forecast run: 2025-11-09 15:00 UTC
.
.
.
.
├── today/
```

### Folder Naming Convention

**Format:** `YYYYMMDDHH`

- **YYYY:** Year (2025)
- **MM:** Month (11 = November)
- **DD:** Day (09 or 10)
- **HH:** Hour in UTC (00, 03, 06, 09, 12, 15, 18, 21)

Each folder represents a **forecast initialization time** and contains all forecast lead times (hours 0-48) for that run.

---
## File Organization

Each forecast run folder contains **1,274 files**:

- **637 EPS files** (Ensemble Prediction System - probabilistic forecasts)
- **637 Deterministic files** (single-value forecasts)
### File Types

#### 1. **EPS Files (Ensemble Probabilistic Forecasts)**

- Contain **20 different ensemble members** representing probabilistic forecast scenarios
- Used for uncertainty quantification and risk assessments
- Identifier: `icon-d2-eps`

#### 2. **Deterministic Files (Median/Control Forecasts)**

- Contain **single median or control forecast** values
- Represent the most likely or average forecast scenario
- Identifier: `icon-d2` (without `-eps`)


---

## File Naming Convention

### General Pattern

```
icon-d2[-eps]_de_lat-lon_{level-type}_{run-time}_{forecast-hour}_{identifier}_{variable}.grb2
```

### Components Explained

|**Component**|**Description**|**Examples**|
|---|---|---|
|**`icon-d2`**|Model identifier (ICON Domain 2)|`icon-d2`, `icon-d2-eps`|
|**`-eps`**|Ensemble Prediction System flag|Present: EPS, Absent: Deterministic|
|**`de`**|Region code (Germany)|`de`|
|**`lat-lon`**|Grid type (regular lat-lon grid)|`lat-lon`|
|**`level-type`**|Vertical level type|`model-level`, `single-level`|
|**`run-time`**|Forecast initialization time (YYYYMMDDHH)|`2025110900`|
|**`forecast-hour`**|Forecast lead time (hours)|`000` to `048`|
|**`identifier`**|Level number or variable dimension|`61`, `62`, `63`, `64`, `2d`|
|**`variable`**|Meteorological variable code|`u`, `v`, `t_2m`, `u_10m`, `v_10m`, etc.|

---

## Data Categories

### 1. Model-Level Wind Data (Upper-Air Winds)

Files containing wind components at specific atmospheric model levels (vertical heights).
#### Deterministic Model-Level Files

```
icon-d2_de_lat-lon_model-level_2025110900_000_61_u.grb2
icon-d2_de_lat-lon_model-level_2025110900_000_61_v.grb2
```

#### EPS Model-Level Files

```
icon-d2-eps_de_lat-lon_model-level_2025110900_000_61_u.grb2
icon-d2-eps_de_lat-lon_model-level_2025110900_000_61_v.grb2
```

  
**Available Model Levels:**

- **Level 61:** ~184m height (approximate - for wind energy at hub heights)
- **Level 62:** ~127m height (approximate)
- **Level 63:** ~78m height (approximate)
- **Level 64:** ~38m height (approximate)

**Variables:**

- **`u`:** Eastward wind component (zonal wind) [m/s]
- **`v`:** Northward wind component (meridional wind) [m/s]

**Note:** These levels correspond to typical wind turbine hub heights and are critical for wind power generation forecasting.

---
### 2. Single-Level Data (Surface Variables)

Files containing meteorological variables at fixed heights or at the surface.
#### Deterministic/EPS Single-Level Files

```
icon-d2_de_lat-lon_single-level_2025110900_000_2d_u_10m.grb2
icon-d2_de_lat-lon_single-level_2025110900_000_2d_v_10m.grb2
icon-d2_de_lat-lon_single-level_2025110900_000_2d_t_2m.grb2
icon-d2_de_lat-lon_single-level_2025110900_000_2d_aswdir_s.grb2
icon-d2_de_lat-lon_single-level_2025110900_000_2d_aswdifd_s.grb2
```

**Available Variables/Metrics:**

| **Variable Code** | **Description**                        | **Unit** | **Use Case**                                  |
| ----------------- | -------------------------------------- | -------- | --------------------------------------------- |
| **`u_10m`**       | Eastward wind component at 10m height  | m/s      | Wind power (small turbines, surface wind)     |
| **`v_10m`**       | Northward wind component at 10m height | m/s      | Wind power (small turbines, surface wind)     |
| **`t_2m`**        | Temperature at 2m height               | K or °C  | Solar panel efficiency, demand forecasting    |
| **`aswdir_s`**    | Direct shortwave radiation at surface  | W/m²     | Solar PV power generation (direct component)  |
| **`aswdifd_s`**   | Diffuse shortwave radiation at surface | W/m²     | Solar PV power generation (diffuse component) |

---
## Forecast Temporal Coverage

### Forecast Lead Times

- **Range:** 0 to 48 hours ahead
- **Interval:** 1 hour
- **Total steps:** 49 forecast hours per run (000, 001, 002, ..., 048)
### Example Timeline

For forecast run `2025110900` (Nov 9, 2025 00:00 UTC):

- **Hour 000:** Valid at 2025-11-09 00:00 UTC (analysis/nowcast)
- **Hour 001:** Valid at 2025-11-09 01:00 UTC
- **Hour 024:** Valid at 2025-11-10 00:00 UTC
- **Hour 048:** Valid at 2025-11-11 00:00 UTC

---

## Data Characteristics

### Ensemble Prediction System (EPS)

**EPS files** (`icon-d2-eps`) contain **20 ensemble members**:

- Each member represents a slightly different initial condition
- Provides probabilistic forecast information
- Enables uncertainty quantification and risk assessment
- Useful for:
	- Calculating forecast spread/uncertainty
	- Generating probabilistic forecasts (e.g., P10, P50, P90 quantiles)
	- Risk-based decision making for energy trading

### Deterministic Forecasts

**Deterministic files** (`icon-d2`) contain:

- Single forecast value (median, control, or best estimate)
- Represents the most likely forecast scenario
- Derived from ensemble mean or control run
- Useful for:
	- Point forecasts
	- Operational planning
	- Baseline scenarios

---
## File Count Summary

**Per forecast run folder:**

- Total files: **1,274**
- EPS files: **637**
- Model-level: ~392 (4 levels × 2 variables × 49 hours)
- Single-level: ~245 (5 variables × 49 hours)
- Deterministic files: **637**
- Model-level: ~392
- Single-level: ~245

---
## Citation and Data Source

**Data Provider:** Deutscher Wetterdienst (DWD)
**Model:** ICON-D2 (ICON Numerical Weather Prediction)
**Website:** https://www.dwd.de/
**Open Data:** https://opendata.dwd.de/
**Metrics Description:** https://www.dwd.de/SharedDocs/downloads/DE/modelldokumentationen/nwv/icon/icon_dbbeschr_aktuell.pdf