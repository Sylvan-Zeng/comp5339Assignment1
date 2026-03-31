# Handover Notes

## Package Purpose

This folder is the final GitHub upload package for Assignment 1 step 1 and step 2.

It contains:

- final code
- final documentation
- generated raw source downloads
- generated cleaned outputs
- DuckDB output

## Main Files

- [assignment1_steps_1_2.py](assignment1_steps_1_2.py)
- [requirements.txt](requirements.txt)
- [README.md](README.md)
- [handover_notes.md](handover_notes.md)
- [data_outputs](data_outputs)

## What Has Been Completed

Completed scope:

- source discovery and download logic
- cleaning and standardisation
- state-year integration
- facility-level integration
- facility-level CER matching revision to reduce obvious false positives

Implemented source groups:

- CER NGER electricity sector emissions and generation
- CER historical accredited renewable power stations
- ABS Data by Region state-level indicators

## Included Data Outputs

Raw downloads:

- [data_outputs/raw](data_outputs/raw)

Processed outputs:

- [nger_facility_clean.csv](data_outputs/processed/nger_facility_clean.csv)
- [cer_power_stations_clean.csv](data_outputs/processed/cer_power_stations_clean.csv)
- [abs_state_measures_clean.csv](data_outputs/processed/abs_state_measures_clean.csv)
- [abs_measure_coverage.csv](data_outputs/processed/abs_measure_coverage.csv)
- [consolidated_facility_year.csv](data_outputs/processed/consolidated_facility_year.csv)
- [integrated_state_year.csv](data_outputs/processed/integrated_state_year.csv)

Database output:

- [assignment1_steps_1_2.duckdb](data_outputs/assignment1_steps_1_2.duckdb)

## Actual Output Size

The pipeline has already been run successfully for this package.

Output counts:

- `NGER facility rows: 4,862`
- `CER power station rows: 3,432`
- `ABS state-year measure rows: 304`
- `Consolidated facility-year rows: 4,862`
- `Integrated state-year rows: 80`

## Default Year Logic

Default reporting periods:

- `2014-15` to `2023-24`

ABS alignment:

- NGER `2014-15` aligns to ABS `2015`
- NGER `2015-16` aligns to ABS `2016`
- ...
- NGER `2023-24` aligns to ABS `2024`

Reason:

- the selected ABS series are year-ended to 30 June
- aligning to the NGER reporting-period end year is the cleaner choice

## ABS Caveat

The current ABS indicators were chosen to reflect economic and industry context, but they do not all span the full ten-year window.

Current ABS indicators:

- `INCOME_4`
- `INCOME_3`
- `INCOME_2`
- `INCOME_21`
- `CABEE_5`
- `CABEE_10`
- `CABEE_15`
- `CABEE_23`

Important consequence:

- ABS enrichment is partial across the ten-year panel
- before modelling or interpretation, check [abs_measure_coverage.csv](data_outputs/processed/abs_measure_coverage.csv)

## Facility-Level Matching Logic

Facility matching between NGER and CER is now handled in two layers:

1. candidate matching
2. validated matching

This was done to preserve all NGER rows while preventing obvious false-positive matches from being treated as confirmed CER joins.

### Why This Exists

NGER and CER do not represent exactly the same universe of facilities.

- NGER includes all designated generation facilities
- CER historical accredited power stations represent the renewable accreditation dataset

Because of this:

- many NGER rows should remain unmatched
- non-renewable NGER rows should not be force-matched into CER
- same-name matches can still be misleading if fuel families or timing do not align

### Match Eligibility

Only renewable-like NGER fuel families are treated as CER-match-eligible:

- `solar`
- `wind`
- `hydro`
- `bioenergy`

Rows outside those fuel families are kept, but marked as outside CER scope.

### Candidate Match Stages

Candidate matching is attempted in this order:

1. exact state + normalized name
2. state + canonicalized name
3. state + reduced core name + fuel family

### Validation Rules

A candidate is only promoted to a confirmed match if:

- the fuel family is consistent
- the CER accreditation start year is not later than the NGER reporting year end

## How To Read The Key Columns

### `candidate_*`

These columns store possible CER matches found by the matching logic.

Examples:

- `candidate_power_station_name`
- `candidate_accreditation_code`
- `candidate_fuel_source`
- `candidate_fuel_family`
- `candidate_accreditation_start_year`

Use case:

- manual review
- debugging
- match improvement in later work

Do not automatically treat `candidate_*` as confirmed joins.

### `matched_*`

These columns store only validated CER matches.

Examples:

- `matched_power_station_name`
- `matched_accreditation_code`
- `matched_fuel_source`
- `matched_fuel_family`
- `matched_accreditation_start_year`

Use case:

- safe downstream station-level enrichment

### `facility_match_found`

- `True`: validated CER match accepted
- `False`: no confirmed CER match

### `facility_match_status`

- `matched`
  - confirmed CER match
- `outside_cer_scope_nonrenewable`
  - row preserved, but not eligible for CER renewable matching
- `unmatched_eligible`
  - row was eligible for CER matching but no candidate was found
- `rejected_fuel_family_mismatch`
  - candidate existed but failed fuel-family consistency
- `rejected_future_accreditation`
  - candidate existed but accreditation timing was later than the NGER period

### `facility_match_method`

How the candidate was identified, for example:

- `exact_name_state`
- `canonical_name_state`
- `core_name_state_fuel`
- `unmatched`
- `outside_cer_scope_nonrenewable`

## How The Next Teammate Should Use The Data

If the next teammate needs a safe CER-enriched facility table:

- use rows where `facility_match_found == True`
- use the `matched_*` fields

If the next teammate wants to improve matching:

- inspect rows where `candidate_power_station_name` is not empty
- but `facility_match_found == False`

If the next teammate is doing step 3 geocoding or station-level enrichment:

1. use `matched_*` first
2. manually review candidate-only rows
3. leave `outside_cer_scope_nonrenewable` rows unjoined to CER unless there is a separate reason to enrich them

## Jupyter Use

If teammate prefers Jupyter, they can import the script directly:

```python
from assignment1_steps_1_2 import *
```

Then run the workflow step by step in notebook cells.

## Remaining Risks

- ABS year coverage is still partial for the selected measures
- facility matching is safer now, but still not a substitute for manual review in high-stakes station-level analysis
- website structure changes at CER or ABS could affect future reruns
