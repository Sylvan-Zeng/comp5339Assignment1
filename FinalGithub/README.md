# COMP5339 Assignment 1: Step 1-2 Final Handover Package

This folder is the GitHub-ready package for Assignment 1 step 1 and step 2.

It includes:

- the final Python pipeline script
- the dependency file
- project documentation
- the generated raw and processed datasets
- the DuckDB database output

## Included Files

- [assignment1_steps_1_2.py](assignment1_steps_1_2.py)
- [requirements.txt](requirements.txt)
- [README.md](README.md)
- [handover_notes.md](handover_notes.md)
- [data_outputs](data_outputs)

## What The Script Does

The pipeline covers:

- Step 1: data acquisition
- Step 2: data cleaning and integration

Data source groups:

- CER NGER electricity sector emissions and generation data
- CER historical accredited renewable power stations data
- ABS Data by Region state-level indicators

The default reporting-period range is:

- NGER: `2014-15` to `2023-24`
- ABS aligned year-ended data: `2015` to `2024`

## Data Acquisition Method

The script uses standard HTTP request/response logic with Python `requests`.

The workflow is:

1. request CER and ABS webpages or file endpoints
2. parse yearly download links where necessary
3. download CSV or XLSX source files
4. clean and standardise the datasets
5. export integrated outputs

This means the implementation follows the class approach of using web requests, even though some sources are webpage-driven rather than JSON APIs.

## Generated Outputs In This Package

The generated datasets are already included in [data_outputs](data_outputs).

Main files:

- [assignment1_steps_1_2.duckdb](data_outputs/assignment1_steps_1_2.duckdb)
- [nger_facility_clean.csv](data_outputs/processed/nger_facility_clean.csv)
- [cer_power_stations_clean.csv](data_outputs/processed/cer_power_stations_clean.csv)
- [abs_state_measures_clean.csv](data_outputs/processed/abs_state_measures_clean.csv)
- [abs_measure_coverage.csv](data_outputs/processed/abs_measure_coverage.csv)
- [consolidated_facility_year.csv](data_outputs/processed/consolidated_facility_year.csv)
- [integrated_state_year.csv](data_outputs/processed/integrated_state_year.csv)

Two most important downstream tables:

- `consolidated_facility_year.csv`: facility-level table for station-level matching, review, and later geospatial work
- `integrated_state_year.csv`: state-year table for aggregated analysis and visualisation

## Actual Output Size

This package was run successfully and produced:

- `NGER facility rows: 4,862`
- `CER power station rows: 3,432`
- `ABS state-year measure rows: 304`
- `Consolidated facility-year rows: 4,862`
- `Integrated state-year rows: 80`

## How To Run Again

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the script:

```bash
python assignment1_steps_1_2.py --output-dir data_outputs
```

## Important Limitation

Two known data limitations remain:

- the selected ABS indicators do not cover the full ten-year span evenly, so `abs_measure_coverage.csv` should be checked before analysis
- facility-level CER matching is safer than before, but still includes a distinction between candidate matches and validated matches; use the notes in [handover_notes.md](handover_notes.md) before relying on station-level joins

## Recommended Use For Teammates

- If the next teammate wants ready-to-use outputs, start from the files in `data_outputs/processed/`
- If want to inspect station-level renewable matching, start from `consolidated_facility_year.csv`
- If want aggregated analysis, start from `integrated_state_year.csv`
- If want to rerun the pipeline, use `assignment1_steps_1_2.py`


# COMP5339 Assignment 1: Step 3
## This extension continues from the outputs of Step 1–2.
The workflow is:

load consolidated_facility_year.csv from data_outputs/processed/
extract unique facility names and states for geocoding
perform geocoding using Google Geocoding API with OpenStreetMap fallback
generate a geocoded lookup table
update the DuckDB database with the geocoded facility table

The following files are generated:

facility_geocoded.csv: unique facility-level latitude and longitude lookup table
assignment1_steps_3.duckdb: updated database with coordinates added to consolidated_facility_year

Differences from Step 1–2 outputs:

consolidated_facility_year now includes latitude and longitude
all other tables remain unchanged


# COMP5339 Assignment 1 – Task 4

## Data Transformation and Storage (DuckDB)

### 1. Overview

This notebook implements **Task 4: Data Transformation and Storage** based on the outputs from Task 1–3.
The goal is to transform the processed datasets into a structured database schema and store them in **DuckDB** for analysis and visualisation.

All input datasets are read from:

```
./data_outputs/processed/
```

The final database file generated is:

```
assignment1_task4.duckdb
```

---

### 2. Input Data

The following files are used as inputs:

* `nger_facility_clean.csv`
* `cer_power_stations_clean.csv`
* `facility_geocoded.csv`
* `abs_state_measures_clean.csv`
* `abs_measure_coverage.csv`
* `integrated_state_year.csv`

These datasets are outputs from Task 1–3, including cleaned, integrated, and augmented data.

---

### 3. Tools and Technologies

* Python 3.10+
* DuckDB
* Pandas
* (Optional) DuckDB Spatial Extension

---

### 4. How to Run

#### Step 1: Install dependencies

```bash
pip install -r requirements.txt
```

#### Step 2: Open the notebook

```bash
jupyter notebook task4_duckdb_transformation.ipynb
```

#### Step 3: Run all cells

* The notebook will:

  * Load all CSV files
  * Create DuckDB database
  * Build schema (dimension + fact tables)
  * Insert transformed data
  * Create views for analysis

---

### 5. Database Design

The database follows a **star schema design**, including:

* **Dimension Tables**

  * State
  * Reporting Period
  * ABS Measures
  * Reporting Entity
  * Facility
  * Power Station

* **Fact Tables**

  * NGER Facility-Year data
  * Facility Location (with coordinates)
  * ABS State Measures
  * State-Year Summary

* **Bridge Table**

  * Facility ↔ Power Station matching

* **Views**

  * State-Year Dashboard
  * Facility Emissions Map

This design improves:

* Data consistency
* Query performance
* Analytical flexibility

---

### 6. Spatial Support

The notebook attempts to install and load DuckDB spatial extension:

```sql
INSTALL spatial;
LOAD spatial;
```

If not supported, the database will still work using latitude and longitude fields.

---

### 7. Outputs

After running the notebook, you will get:

* `assignment1_task4.duckdb`
* Structured relational schema
* Analytical views ready for visualisation

---

### 8. Notes

* All transformations are reproducible from the notebook
* No manual editing is required
* The schema is designed to support **Assignment 2 spatial queries**

---

