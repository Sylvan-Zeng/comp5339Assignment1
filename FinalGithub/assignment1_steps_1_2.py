from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import duckdb
import pandas as pd
import requests
from bs4 import BeautifulSoup


NGER_INDEX_URL = "https://cer.gov.au/markets/reports-and-data/nger-reporting-data-and-registers"
CER_POWER_STATIONS_CSV_URL = "https://cer.gov.au/document/historical-accredited-power-stations-and-projects-0"
ABS_BASE_URL = "https://dbr.abs.gov.au/processedData/csv"

DEFAULT_START_YEAR = 2014
DEFAULT_END_YEAR = 2024
ABS_LAYER = "STE"
ABS_STATE_CODES = {
    "NSW": "1",
    "VIC": "2",
    "QLD": "3",
    "SA": "4",
    "WA": "5",
    "TAS": "6",
    "NT": "7",
    "ACT": "8",
}
ABS_DEFAULT_MEASURE_CODES = {
    "INCOME_4": "employee_income_earners_count",
    "INCOME_3": "total_employee_income_million_aud",
    "INCOME_2": "median_employee_income_aud",
    "INCOME_21": "mean_employee_income_aud",
    "CABEE_5": "total_businesses_count",
    "CABEE_10": "business_entries_count",
    "CABEE_15": "business_exits_count",
    "CABEE_23": "utilities_businesses_count",
}
LEGAL_NAME_TOKENS = {
    "pty",
    "ltd",
    "limited",
    "proprietary",
    "corporation",
    "corp",
    "company",
    "co",
}
GENERIC_FACILITY_TOKENS = {
    "solar",
    "wind",
    "hydro",
    "power",
    "station",
    "farm",
    "plant",
    "battery",
    "renewable",
    "energy",
    "project",
    "projects",
    "development",
    "developments",
}
STATE_NAME_TOKENS = {
    "nsw",
    "vic",
    "qld",
    "sa",
    "wa",
    "tas",
    "nt",
    "act",
}
MATCHED_POWER_STATION_COLUMNS = [
    "matched_power_station_name",
    "matched_accreditation_code",
    "matched_installed_capacity_mw",
    "matched_postcode",
    "matched_fuel_source",
    "matched_accreditation_start_date",
    "matched_accreditation_start_year",
    "matched_suspension_status",
    "matched_baseline_mwh",
    "matched_fuel_family",
]
CANDIDATE_POWER_STATION_COLUMNS = [
    "candidate_power_station_name",
    "candidate_accreditation_code",
    "candidate_installed_capacity_mw",
    "candidate_postcode",
    "candidate_fuel_source",
    "candidate_accreditation_start_date",
    "candidate_accreditation_start_year",
    "candidate_suspension_status",
    "candidate_baseline_mwh",
    "candidate_fuel_family",
]
CER_MATCHABLE_FUEL_FAMILIES = {"solar", "wind", "hydro", "bioenergy"}


@dataclass(frozen=True)
class ReportingPeriod:
    start_year: int
    end_year: int

    @property
    def label(self) -> str:
        return f"{self.start_year}-{str(self.end_year)[-2:]}"


def parse_args() -> argparse.Namespace:
    default_output_dir = str((Path(__file__).resolve().parent / "data_outputs"))
    parser = argparse.ArgumentParser(
        description="COMP5339 assignment pipeline for step 1 (data acquisition) and step 2 (integration and cleaning)."
    )
    parser.add_argument(
        "--output-dir",
        default=default_output_dir,
        help="Directory where raw, processed, and database outputs will be written. Defaults to a data_outputs folder beside this script.",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=DEFAULT_START_YEAR,
        help="First reporting period start year, inclusive. Default 2014 produces 2014-15 as the first period.",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=DEFAULT_END_YEAR,
        help="Last reporting period end year, inclusive. Default 2024 produces 2023-24 as the last period.",
    )
    parser.add_argument(
        "--database-name",
        default="assignment1_steps_1_2.duckdb",
        help="DuckDB file name created under the output directory.",
    )
    return parser.parse_args()


def build_reporting_periods(start_year: int, end_year: int) -> list[ReportingPeriod]:
    if end_year <= start_year:
        raise ValueError("end-year must be greater than start-year.")
    periods: list[ReportingPeriod] = []
    for year in range(start_year, end_year):
        periods.append(ReportingPeriod(start_year=year, end_year=year + 1))
    return periods


def normalize_column_name(value: str) -> str:
    value = value.replace("\n", " ").replace("\r", " ")
    value = re.sub(r"[%/()\-]+", " ", value)
    value = re.sub(r"[^0-9a-zA-Z]+", "_", value.strip().lower())
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def clean_numeric_series(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.replace(",", "", regex=False).replace({"-": pd.NA, "": pd.NA})
    return pd.to_numeric(cleaned, errors="coerce")


def normalize_name_key(value: str) -> str:
    value = str(value).strip().lower()
    value = value.replace("&", " and ")
    value = re.sub(r"\bmt\b\.?", "mount", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def canonicalize_facility_name(value: str) -> str:
    value = str(value).strip().lower()
    value = value.replace("&", " and ")
    value = re.sub(r"\bmt\b\.?", "mount", value)
    value = re.sub(r"\s+-\s+(solar|wind|hydro|battery|biomass|gas|coal|diesel|oil).*$", "", value)
    value = re.sub(r"\s+-\s+(nsw|vic|qld|sa|wa|tas|nt|act)$", "", value)
    value = re.sub(r"\b(?:pty|ltd|limited|proprietary|corporation|corp|company|co)\b", " ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    tokens = [token for token in value.split() if token not in STATE_NAME_TOKENS]
    return " ".join(tokens)


def build_name_core(value: str) -> str:
    canonical = canonicalize_facility_name(value)
    tokens = [token for token in canonical.split() if token not in GENERIC_FACILITY_TOKENS and token not in LEGAL_NAME_TOKENS]
    return " ".join(tokens)


def map_fuel_family(value: str) -> str:
    text = str(value).strip().lower()
    if not text or text == "nan":
        return "unknown"
    if "solar" in text:
        return "solar"
    if "wind" in text:
        return "wind"
    if "hydro" in text:
        return "hydro"
    if any(token in text for token in ["biomass", "biogas", "bagasse", "landfill", "waste"]):
        return "bioenergy"
    if "battery" in text:
        return "battery"
    if "gas" in text:
        return "gas"
    if "coal" in text:
        return "coal"
    if any(token in text for token in ["diesel", "oil", "kerosene", "petrol"]):
        return "liquid_fuel"
    return "other"


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=120)
    response.raise_for_status()
    return response.text


def download_file(session: requests.Session, url: str, destination: Path) -> Path:
    if destination.exists():
        return destination
    response = session.get(url, timeout=120)
    response.raise_for_status()
    destination.write_bytes(response.content)
    return destination


def find_document_link(html: str, base_url: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    csv_candidate: str | None = None
    xlsx_candidate: str | None = None
    for anchor in soup.select("a[href]"):
        href = anchor.get("href", "").strip()
        text = " ".join(anchor.get_text(" ", strip=True).split()).lower()
        if "/document/" not in href:
            continue
        full_url = urljoin(base_url, href)
        if "csv" in text:
            csv_candidate = full_url
        if "xlsx" in text or "xls" in text:
            xlsx_candidate = full_url
    if csv_candidate:
        return csv_candidate, ".csv"
    if xlsx_candidate:
        return xlsx_candidate, ".xlsx"
    raise RuntimeError(f"Could not find a downloadable CSV/XLSX link on {base_url}")


def read_tabular_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".xlsx":
        preview = pd.read_excel(path, header=None)
        header_row = 0
        for row_index in range(min(len(preview), 10)):
            normalized_row = [normalize_column_name(str(value)) for value in preview.iloc[row_index].tolist()]
            if "facility_name" in normalized_row and ("reporting_entity" in normalized_row or "state" in normalized_row):
                header_row = row_index
                break
        return pd.read_excel(path, header=header_row)
    last_error: Exception | None = None
    for encoding in ["utf-8", "utf-8-sig", "cp1252", "latin1"]:
        try:
            return pd.read_csv(path, encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    return pd.read_csv(path)


def harmonize_nger_columns(frame: pd.DataFrame) -> pd.DataFrame:
    aliases = {
        "reporting_entity": ["reporting_entity", "controlling_corporation"],
        "total_scope_1_emissions_t_co2_e": ["total_scope_1_emissions_t_co2_e", "scope_1_t_co2_e"],
        "total_scope_2_emissions_t_co2_e": [
            "total_scope_2_emissions_t_co2_e",
            "total_scope_2_emissions_t_co2_e_2",
            "scope_2_t_co2_e",
        ],
        "emission_intensity_t_co2_e_mwh": [
            "emission_intensity_t_co2_e_mwh",
            "emission_intensity_t_mwh",
        ],
        "grid_connected": ["grid_connected", "grid_connected2"],
    }
    rename_map: dict[str, str] = {}
    for canonical_name, candidates in aliases.items():
        if canonical_name in frame.columns:
            continue
        for candidate in candidates:
            if candidate in frame.columns:
                rename_map[candidate] = canonical_name
                break
    if rename_map:
        frame = frame.rename(columns=rename_map)
    return frame


def extract_nger_period_pages(session: requests.Session) -> dict[str, str]:
    html = fetch_html(session, NGER_INDEX_URL)
    soup = BeautifulSoup(html, "html.parser")
    period_pages: dict[str, str] = {}
    pattern = re.compile(
        r"Electricity sector emissions and generation(?: data)?\s+(\d{4})\D+(\d{2})",
        re.IGNORECASE,
    )
    for anchor in soup.select("a[href]"):
        text = " ".join(anchor.get_text(" ", strip=True).split())
        match = pattern.search(text)
        if not match:
            continue
        label = f"{match.group(1)}-{match.group(2)}"
        href = anchor.get("href", "").strip()
        if href:
            period_pages[label] = urljoin(NGER_INDEX_URL, href)
    return period_pages


def acquire_nger_files(
    session: requests.Session,
    periods: Iterable[ReportingPeriod],
    raw_dir: Path,
) -> list[tuple[ReportingPeriod, Path]]:
    period_pages = extract_nger_period_pages(session)
    downloaded: list[tuple[ReportingPeriod, Path]] = []
    for period in periods:
        page_url = period_pages.get(period.label)
        if not page_url:
            raise RuntimeError(f"Could not find an NGER year page for reporting period {period.label}.")
        year_page_html = fetch_html(session, page_url)
        file_url, extension = find_document_link(year_page_html, page_url)
        destination = raw_dir / f"nger_electricity_{period.label}{extension}"
        download_file(session, file_url, destination)
        downloaded.append((period, destination))
    return downloaded


def clean_nger_files(downloads: Iterable[tuple[ReportingPeriod, Path]]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for period, csv_path in downloads:
        frame = read_tabular_file(csv_path)
        frame.columns = [normalize_column_name(column) for column in frame.columns]
        frame = harmonize_nger_columns(frame)
        frame["reporting_period"] = period.label
        frame["reporting_year_start"] = period.start_year
        frame["reporting_year_end"] = period.end_year
        frame["abs_reference_year"] = period.end_year
        frame["source_file"] = csv_path.name
        if "type" in frame.columns:
            frame = frame[frame["type"].eq("F")].copy()
        numeric_columns = [
            "electricity_production_gj",
            "electricity_production_mwh",
            "total_scope_1_emissions_t_co2_e",
            "total_scope_2_emissions_t_co2_e",
            "total_emissions_t_co2_e",
            "emission_intensity_t_co2_e_mwh",
        ]
        for column in numeric_columns:
            if column in frame.columns:
                frame[column] = clean_numeric_series(frame[column])
        frame["state"] = frame["state"].astype(str).str.strip().str.upper()
        frame["facility_name_key"] = frame["facility_name"].map(normalize_name_key)
        frame["facility_name_canonical"] = frame["facility_name"].map(canonicalize_facility_name)
        frame["facility_name_core"] = frame["facility_name"].map(build_name_core)
        frame["reporting_entity_key"] = frame["reporting_entity"].astype(str).str.strip().str.lower()
        frame["primary_fuel_family"] = frame["primary_fuel"].map(map_fuel_family)
        frames.append(frame)
    if not frames:
        raise RuntimeError("No NGER files were cleaned.")
    combined = pd.concat(frames, ignore_index=True)
    combined["nger_row_id"] = range(1, len(combined) + 1)
    return combined.sort_values(["reporting_year_end", "state", "reporting_entity", "facility_name"]).reset_index(drop=True)


def acquire_power_station_file(session: requests.Session, raw_dir: Path) -> Path:
    destination = raw_dir / "cer_historical_accredited_power_stations.csv"
    return download_file(session, CER_POWER_STATIONS_CSV_URL, destination)


def clean_power_station_file(csv_path: Path) -> pd.DataFrame:
    frame = pd.read_csv(csv_path)
    frame.columns = [normalize_column_name(column) for column in frame.columns]
    frame["state"] = frame["state"].astype(str).str.strip().str.upper()
    frame["installed_capacity"] = clean_numeric_series(frame["installed_capacity"])
    frame["baseline_mwh"] = clean_numeric_series(frame["baseline_mwh"])
    frame["accreditation_start_date"] = pd.to_datetime(frame["accreditation_start_date"], dayfirst=True, errors="coerce")
    frame["accreditation_start_year"] = frame["accreditation_start_date"].dt.year
    frame["power_station_name_key"] = frame["power_station_name"].map(normalize_name_key)
    frame["power_station_name_canonical"] = frame["power_station_name"].map(canonicalize_facility_name)
    frame["power_station_name_core"] = frame["power_station_name"].map(build_name_core)
    frame["suspension_status"] = frame["suspension_status"].astype(str).str.strip()
    frame["fuel_family"] = frame["fuel_source_s"].map(map_fuel_family)
    frame["source_file"] = csv_path.name
    return frame.sort_values(["state", "power_station_name"]).reset_index(drop=True)


def acquire_abs_files(session: requests.Session, raw_dir: Path) -> dict[str, Path]:
    files: dict[str, Path] = {}
    for state, code in ABS_STATE_CODES.items():
        destination = raw_dir / f"abs_{ABS_LAYER.lower()}_{code}_{state.lower()}.csv"
        url = f"{ABS_BASE_URL}/{ABS_LAYER}_{code}.csv"
        download_file(session, url, destination)
        files[state] = destination
    return files


def clean_abs_files(files: dict[str, Path], year_min: int, year_max: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    year_columns = [str(year) for year in range(year_min, year_max + 1)]
    keep_measures = list(ABS_DEFAULT_MEASURE_CODES.keys())
    for state, csv_path in files.items():
        frame = pd.read_csv(csv_path)
        frame.columns = [normalize_column_name(column) for column in frame.columns]
        frame = frame[frame["measure_code"].isin(keep_measures)].copy()
        available_year_columns = [column for column in year_columns if column in frame.columns]
        if not available_year_columns:
            raise RuntimeError(f"No ABS year columns found in {csv_path.name} for range {year_min}-{year_max}.")
        frame = frame.melt(
            id_vars=["measure_code", "parent_description", "description"],
            value_vars=available_year_columns,
            var_name="abs_reference_year",
            value_name="value",
        )
        frame["state"] = state
        frame["abs_reference_year"] = frame["abs_reference_year"].astype(int)
        frame["value"] = clean_numeric_series(frame["value"])
        frame["measure_name"] = frame["measure_code"].map(ABS_DEFAULT_MEASURE_CODES)
        frame["source_file"] = csv_path.name
        frames.append(frame.dropna(subset=["value"]))
    combined = pd.concat(frames, ignore_index=True)
    return combined.sort_values(["state", "measure_code", "abs_reference_year"]).reset_index(drop=True)


def build_abs_state_year_wide(abs_measures: pd.DataFrame) -> pd.DataFrame:
    wide = (
        abs_measures.pivot_table(
            index=["state", "abs_reference_year"],
            columns="measure_name",
            values="value",
            aggfunc="first",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    return wide


def build_abs_measure_coverage(abs_measures: pd.DataFrame, year_min: int, year_max: int) -> pd.DataFrame:
    expected_year_count = year_max - year_min + 1
    coverage = (
        abs_measures.groupby(["measure_code", "measure_name", "parent_description", "description"], dropna=False)
        .agg(
            first_available_year=("abs_reference_year", "min"),
            last_available_year=("abs_reference_year", "max"),
            available_year_count=("abs_reference_year", "nunique"),
            state_count=("state", "nunique"),
        )
        .reset_index()
    )
    coverage["expected_year_count"] = expected_year_count
    coverage["coverage_ratio"] = coverage["available_year_count"] / expected_year_count
    return coverage.sort_values(["measure_code"]).reset_index(drop=True)


def build_power_station_lookup(power_stations: pd.DataFrame, key_column: str, use_fuel_family: bool) -> pd.DataFrame:
    lookup = power_stations.copy()
    lookup = lookup[lookup[key_column].astype(str).str.len().gt(0)].copy()
    lookup["is_unsuspended"] = lookup["suspension_status"].astype(str).str.lower().eq("unsuspended")
    sort_columns = ["state", key_column]
    ascending = [True, True]
    dedupe_columns = ["state", key_column]
    if use_fuel_family:
        sort_columns.append("fuel_family")
        ascending.append(True)
        dedupe_columns.append("fuel_family")
    sort_columns.extend(["is_unsuspended", "installed_capacity", "power_station_name"])
    ascending.extend([False, False, True])
    lookup = lookup.sort_values(sort_columns, ascending=ascending, na_position="last")
    lookup = lookup.drop_duplicates(subset=dedupe_columns, keep="first")
    selected_columns = dedupe_columns.copy()
    if "fuel_family" not in selected_columns:
        selected_columns.append("fuel_family")
    selected_columns.extend(
        [
            "power_station_name",
            "accreditation_code",
            "installed_capacity",
            "postcode",
            "fuel_source_s",
            "accreditation_start_date",
            "accreditation_start_year",
            "suspension_status",
            "baseline_mwh",
        ]
    )
    lookup = lookup[selected_columns].copy()
    lookup["candidate_fuel_family"] = lookup["fuel_family"]
    return lookup.rename(
        columns={
            "power_station_name": "candidate_power_station_name",
            "accreditation_code": "candidate_accreditation_code",
            "installed_capacity": "candidate_installed_capacity_mw",
            "postcode": "candidate_postcode",
            "fuel_source_s": "candidate_fuel_source",
            "accreditation_start_date": "candidate_accreditation_start_date",
            "accreditation_start_year": "candidate_accreditation_start_year",
            "suspension_status": "candidate_suspension_status",
            "baseline_mwh": "candidate_baseline_mwh",
        }
    )


def finalize_candidate_matches(frame: pd.DataFrame) -> pd.DataFrame:
    finalized = frame.copy()
    for column in MATCHED_POWER_STATION_COLUMNS:
        finalized[column] = pd.NA

    candidate_exists = finalized["candidate_power_station_name"].notna()
    fuel_family_match = finalized["primary_fuel_family"].eq(finalized["candidate_fuel_family"])
    accreditation_year_valid = finalized["candidate_accreditation_start_year"].isna() | (
        pd.to_numeric(finalized["candidate_accreditation_start_year"], errors="coerce")
        <= pd.to_numeric(finalized["reporting_year_end"], errors="coerce")
    )
    accepted = candidate_exists & fuel_family_match & accreditation_year_valid
    rejected_fuel_mismatch = candidate_exists & ~fuel_family_match
    rejected_future_accreditation = candidate_exists & fuel_family_match & ~accreditation_year_valid

    copy_map = {
        "candidate_power_station_name": "matched_power_station_name",
        "candidate_accreditation_code": "matched_accreditation_code",
        "candidate_installed_capacity_mw": "matched_installed_capacity_mw",
        "candidate_postcode": "matched_postcode",
        "candidate_fuel_source": "matched_fuel_source",
        "candidate_accreditation_start_date": "matched_accreditation_start_date",
        "candidate_accreditation_start_year": "matched_accreditation_start_year",
        "candidate_suspension_status": "matched_suspension_status",
        "candidate_baseline_mwh": "matched_baseline_mwh",
        "candidate_fuel_family": "matched_fuel_family",
    }
    for candidate_column, matched_column in copy_map.items():
        finalized.loc[accepted, matched_column] = finalized.loc[accepted, candidate_column]

    finalized["facility_match_found"] = accepted
    finalized["facility_match_status"] = finalized["facility_match_status"].fillna("unmatched_eligible")
    finalized.loc[accepted, "facility_match_status"] = "matched"
    finalized.loc[rejected_fuel_mismatch, "facility_match_status"] = "rejected_fuel_family_mismatch"
    finalized.loc[rejected_future_accreditation, "facility_match_status"] = "rejected_future_accreditation"
    finalized["matched_is_renewable_power_station"] = finalized["facility_match_found"]
    return finalized


def match_facilities_to_power_stations(nger: pd.DataFrame, power_stations: pd.DataFrame) -> pd.DataFrame:
    base_columns = list(nger.columns)
    matched_frames: list[pd.DataFrame] = []
    eligible = nger[nger["primary_fuel_family"].isin(CER_MATCHABLE_FUEL_FAMILIES)].copy()
    outside_scope = nger[~nger["primary_fuel_family"].isin(CER_MATCHABLE_FUEL_FAMILIES)].copy()
    remaining = eligible.copy()
    match_steps = [
        ("exact_name_state", "facility_name_key", "power_station_name_key", False),
        ("canonical_name_state", "facility_name_canonical", "power_station_name_canonical", False),
        ("core_name_state_fuel", "facility_name_core", "power_station_name_core", True),
    ]
    for match_method, left_key, right_key, use_fuel_family in match_steps:
        lookup = build_power_station_lookup(power_stations, right_key, use_fuel_family)
        left_on = ["state", left_key]
        right_on = ["state", right_key]
        if use_fuel_family:
            left_on.append("primary_fuel_family")
            right_on.append("fuel_family")
        merged = remaining.merge(lookup, left_on=left_on, right_on=right_on, how="left")
        matched = merged[merged["candidate_power_station_name"].notna()].copy()
        if not matched.empty:
            matched = matched.drop(columns=[column for column in [right_key, "fuel_family"] if column in matched.columns])
            matched["facility_match_method"] = match_method
            matched["facility_match_status"] = pd.NA
            matched_frames.append(matched)
        remaining = merged[merged["candidate_power_station_name"].isna()][base_columns].copy()
    for column in CANDIDATE_POWER_STATION_COLUMNS:
        remaining[column] = pd.NA
    remaining["facility_match_method"] = "unmatched"
    remaining["facility_match_status"] = "unmatched_eligible"
    matched_frames.append(remaining)
    for column in CANDIDATE_POWER_STATION_COLUMNS:
        outside_scope[column] = pd.NA
    outside_scope["facility_match_method"] = "outside_cer_scope_nonrenewable"
    outside_scope["facility_match_status"] = "outside_cer_scope_nonrenewable"
    matched_frames.append(outside_scope)
    normalized_frames: list[pd.DataFrame] = []
    normalized_columns = base_columns + CANDIDATE_POWER_STATION_COLUMNS + [
        "facility_match_method",
        "facility_match_status",
    ]
    for frame in matched_frames:
        normalized = frame.copy()
        for column in normalized_columns:
            if column not in normalized.columns:
                normalized[column] = pd.Series(pd.NA, index=normalized.index, dtype="object")
            elif column in CANDIDATE_POWER_STATION_COLUMNS or column in {"facility_match_method", "facility_match_status"}:
                normalized[column] = normalized[column].astype("object")
        normalized_frames.append(normalized)
    combined = pd.concat(normalized_frames, ignore_index=True)
    combined = finalize_candidate_matches(combined)
    return combined.sort_values(["nger_row_id"]).reset_index(drop=True)


def build_consolidated_facility_year(nger: pd.DataFrame, power_stations: pd.DataFrame, abs_state_year: pd.DataFrame) -> pd.DataFrame:
    matched = match_facilities_to_power_stations(nger, power_stations)
    consolidated = matched.merge(abs_state_year, on=["state", "abs_reference_year"], how="left")
    return consolidated.sort_values(["reporting_year_end", "state", "reporting_entity", "facility_name"]).reset_index(drop=True)


def aggregate_nger_state_year(nger: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        nger.groupby(
            ["state", "reporting_period", "reporting_year_start", "reporting_year_end", "abs_reference_year"],
            dropna=False,
        )
        .agg(
            facility_count=("facility_name", "nunique"),
            reporting_entity_count=("reporting_entity", "nunique"),
            electricity_production_mwh=("electricity_production_mwh", "sum"),
            electricity_production_gj=("electricity_production_gj", "sum"),
            total_scope_1_emissions_t_co2_e=("total_scope_1_emissions_t_co2_e", "sum"),
            total_scope_2_emissions_t_co2_e=("total_scope_2_emissions_t_co2_e", "sum"),
            total_emissions_t_co2_e=("total_emissions_t_co2_e", "sum"),
        )
        .reset_index()
    )
    grouped["emissions_intensity_t_co2_e_per_mwh"] = grouped["total_emissions_t_co2_e"] / grouped["electricity_production_mwh"]
    return grouped


def build_renewable_capacity_by_state_year(power_stations: pd.DataFrame, periods: Iterable[ReportingPeriod]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    unsuspended = power_stations[power_stations["suspension_status"].str.lower().eq("unsuspended")].copy()
    for period in periods:
        abs_reference_year = period.end_year
        eligible = unsuspended[unsuspended["accreditation_start_year"].le(abs_reference_year)].copy()
        grouped = (
            eligible.groupby("state", dropna=False)
            .agg(
                cumulative_renewable_power_station_count=("power_station_name", "nunique"),
                cumulative_renewable_capacity_mw=("installed_capacity", "sum"),
            )
            .reset_index()
        )
        grouped["reporting_period"] = period.label
        grouped["reporting_year_start"] = period.start_year
        grouped["reporting_year_end"] = period.end_year
        grouped["abs_reference_year"] = abs_reference_year
        rows.extend(grouped.to_dict(orient="records"))
    return pd.DataFrame(rows)


def build_integrated_state_year(
    nger_state_year: pd.DataFrame,
    renewable_capacity: pd.DataFrame,
    abs_state_year: pd.DataFrame,
) -> pd.DataFrame:
    integrated = nger_state_year.merge(
        renewable_capacity[
            [
                "state",
                "abs_reference_year",
                "cumulative_renewable_power_station_count",
                "cumulative_renewable_capacity_mw",
            ]
        ],
        on=["state", "abs_reference_year"],
        how="left",
    )
    integrated = integrated.merge(abs_state_year, on=["state", "abs_reference_year"], how="left")
    if "total_businesses_count" in integrated.columns:
        integrated["renewable_capacity_mw_per_1000_businesses"] = (
            integrated["cumulative_renewable_capacity_mw"] / integrated["total_businesses_count"] * 1000
        )
    if "employee_income_earners_count" in integrated.columns:
        integrated["electricity_mwh_per_income_earner"] = (
            integrated["electricity_production_mwh"] / integrated["employee_income_earners_count"]
        )
        integrated["emissions_t_co2_e_per_income_earner"] = (
            integrated["total_emissions_t_co2_e"] / integrated["employee_income_earners_count"]
        )
    return integrated.sort_values(["reporting_year_end", "state"]).reset_index(drop=True)


def write_outputs(
    output_dir: Path,
    database_name: str,
    nger: pd.DataFrame,
    power_stations: pd.DataFrame,
    abs_measures: pd.DataFrame,
    abs_coverage: pd.DataFrame,
    consolidated_facility_year: pd.DataFrame,
    integrated_state_year: pd.DataFrame,
) -> None:
    processed_dir = output_dir / "processed"
    ensure_directory(processed_dir)

    nger.to_csv(processed_dir / "nger_facility_clean.csv", index=False)
    power_stations.to_csv(processed_dir / "cer_power_stations_clean.csv", index=False)
    abs_measures.to_csv(processed_dir / "abs_state_measures_clean.csv", index=False)
    abs_coverage.to_csv(processed_dir / "abs_measure_coverage.csv", index=False)
    consolidated_facility_year.to_csv(processed_dir / "consolidated_facility_year.csv", index=False)
    integrated_state_year.to_csv(processed_dir / "integrated_state_year.csv", index=False)

    database_path = output_dir / database_name
    with duckdb.connect(str(database_path)) as con:
        con.register("nger_df", nger)
        con.register("power_stations_df", power_stations)
        con.register("abs_measures_df", abs_measures)
        con.register("abs_coverage_df", abs_coverage)
        con.register("facility_year_df", consolidated_facility_year)
        con.register("state_year_df", integrated_state_year)
        con.execute("CREATE OR REPLACE TABLE nger_facilities AS SELECT * FROM nger_df")
        con.execute("CREATE OR REPLACE TABLE cer_power_stations AS SELECT * FROM power_stations_df")
        con.execute("CREATE OR REPLACE TABLE abs_state_measures AS SELECT * FROM abs_measures_df")
        con.execute("CREATE OR REPLACE TABLE abs_measure_coverage AS SELECT * FROM abs_coverage_df")
        con.execute("CREATE OR REPLACE TABLE consolidated_facility_year AS SELECT * FROM facility_year_df")
        con.execute("CREATE OR REPLACE TABLE integrated_state_year AS SELECT * FROM state_year_df")


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    raw_nger_dir = output_dir / "raw" / "nger"
    raw_cer_dir = output_dir / "raw" / "cer"
    raw_abs_dir = output_dir / "raw" / "abs"
    for directory in [raw_nger_dir, raw_cer_dir, raw_abs_dir]:
        ensure_directory(directory)

    periods = build_reporting_periods(args.start_year, args.end_year)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "COMP5339-Assignment1-Step1-2-Pipeline/1.0",
            "Accept": "*/*",
        }
    )

    nger_downloads = acquire_nger_files(session, periods, raw_nger_dir)
    nger_clean = clean_nger_files(nger_downloads)

    power_station_path = acquire_power_station_file(session, raw_cer_dir)
    power_stations_clean = clean_power_station_file(power_station_path)

    # ABS series are year-ended 30 June, so NGER 2014-15 aligns to ABS 2015.
    abs_year_min = args.start_year + 1
    abs_year_max = args.end_year
    abs_files = acquire_abs_files(session, raw_abs_dir)
    abs_clean = clean_abs_files(abs_files, year_min=abs_year_min, year_max=abs_year_max)
    abs_state_year = build_abs_state_year_wide(abs_clean)
    abs_coverage = build_abs_measure_coverage(abs_clean, year_min=abs_year_min, year_max=abs_year_max)

    consolidated_facility_year = build_consolidated_facility_year(nger_clean, power_stations_clean, abs_state_year)
    nger_state_year = aggregate_nger_state_year(nger_clean)
    renewable_capacity = build_renewable_capacity_by_state_year(power_stations_clean, periods)
    integrated_state_year = build_integrated_state_year(nger_state_year, renewable_capacity, abs_state_year)

    write_outputs(
        output_dir=output_dir,
        database_name=args.database_name,
        nger=nger_clean,
        power_stations=power_stations_clean,
        abs_measures=abs_clean,
        abs_coverage=abs_coverage,
        consolidated_facility_year=consolidated_facility_year,
        integrated_state_year=integrated_state_year,
    )

    print(f"Finished step 1 and step 2 outputs under: {output_dir}")
    print(f"Processed CSV directory: {output_dir / 'processed'}")
    print(f"DuckDB file: {output_dir / args.database_name}")
    print(f"NGER facility rows: {len(nger_clean):,}")
    print(f"CER power station rows: {len(power_stations_clean):,}")
    print(f"ABS state-year measure rows: {len(abs_clean):,}")
    print(f"Consolidated facility-year rows: {len(consolidated_facility_year):,}")
    print(f"Integrated state-year rows: {len(integrated_state_year):,}")


if __name__ == "__main__":
    main()
