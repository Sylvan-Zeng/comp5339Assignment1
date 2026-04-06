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
