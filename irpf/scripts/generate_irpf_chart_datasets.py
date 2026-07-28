#!/usr/bin/env python3
"""Generate the browser datasets from the audited IRPF master dataset."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


IRPF_DIR = Path(__file__).resolve().parents[1]
RAW_ORIGINAL_PATH = IRPF_DIR / "IRPF_tablas_1990-2023.json"
AUDITED_MASTER_PATH = IRPF_DIR / "IRPF_tablas_1990-2023_completo.json"
VERIFIED_JSON_PATH = IRPF_DIR / "IRPF_tramos_estatales_1990-2026_verificado.json"
VERIFIED_CSV_PATH = IRPF_DIR / "IRPF_tramos_estatales_1990-2026.csv"
INDICATORS_PATH = IRPF_DIR / "indicadores_estado_a_1986.csv"
OUTPUT_PATH = IRPF_DIR / "irpf_chart_datasets.js"
PESETAS_PER_EURO = 166.386
DEFLATOR_1990_TO_1986 = 0.8


def parse_es_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or "adelante" in text.lower():
        return None
    normalized = text.replace("\u00a0", "").replace(" ", "")
    if "," in normalized:
        normalized = normalized.replace(".", "").replace(",", ".")
    else:
        normalized = normalized.replace(".", "")
    return float(normalized)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_indicators(path: Path = INDICATORS_PATH) -> dict[int, dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return {int(row["anio"]): row for row in csv.DictReader(handle)}


def optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def state_entry(record: dict[str, Any]) -> dict[str, Any]:
    matches = [
        entry
        for entry in record["entries"]
        if entry.get("scope") == "state"
        and entry.get("table_kind") in {"general_only", "state_general"}
    ]
    if len(matches) != 1:
        raise ValueError(
            f"{record['year']}: expected one state/general scale, found {len(matches)}"
        )
    return matches[0]


def convert_amount(value: float | None, year: int) -> float | None:
    if value is None:
        return None
    return value / PESETAS_PER_EURO if year <= 2001 else value


def normalize_record(
    record: dict[str, Any], indicators: dict[int, dict[str, str]]
) -> dict[str, Any]:
    year = int(record["year"])
    indicator = indicators[year]
    deflator_to_1986 = float(indicator["deflactor_hasta_1986"])
    deflator_to_1990 = deflator_to_1986 / DEFLATOR_1990_TO_1986
    gdp_pc_nominal = float(indicator["pib_per_capita_eur_corriente"])
    entry = state_entry(record)
    brackets: list[dict[str, Any]] = []

    for index, row in enumerate(entry["rows"], start=1):
        lower_original = parse_es_number(row.get("base_hasta"))
        remainder_original = parse_es_number(row.get("resto_base"))
        is_open = remainder_original is None
        upper_original = (
            None
            if is_open
            else (lower_original or 0.0) + remainder_original
        )
        rate = parse_es_number(row.get("tipo_aplicable"))
        lower_nominal = convert_amount(lower_original, year)
        upper_nominal = convert_amount(upper_original, year)
        brackets.append(
            {
                "index": index,
                "rate": rate,
                "lower_original": lower_original,
                "upper_original": upper_original,
                "lower_nominal_eur": lower_nominal,
                "upper_nominal_eur": upper_nominal,
                "lower_real_eur_1990": (
                    lower_nominal * deflator_to_1990
                    if lower_nominal is not None
                    else None
                ),
                "upper_real_eur_1990": (
                    upper_nominal * deflator_to_1990
                    if upper_nominal is not None
                    else None
                ),
                "is_open": is_open,
            }
        )

    finite_limits = [
        bracket["upper_nominal_eur"]
        for bracket in brackets
        if bracket["upper_nominal_eur"] is not None
    ]
    if any(right <= left for left, right in zip(finite_limits, finite_limits[1:])):
        raise ValueError(f"{year}: finite upper limits are not strictly increasing")
    if any(
        bracket["rate"] is None or not 0 <= bracket["rate"] <= 100
        for bracket in brackets
    ):
        raise ValueError(f"{year}: invalid marginal rate")

    return {
        "year": year,
        "regime": record["regime"],
        "deflator_hasta_1986": deflator_to_1986,
        "deflator_hasta_1990": deflator_to_1990,
        "original_currency": "pesetas" if year <= 2001 else "euros",
        "source_url": record["source_url"],
        "row_count": len(brackets),
        "finite_bracket_count": len(finite_limits),
        "gdp_pc_nominal_eur": gdp_pc_nominal,
        "gdp_pc_real_eur_1990": gdp_pc_nominal * deflator_to_1990,
        "brackets": brackets,
    }


def normalize_verified_record(
    record: dict[str, Any], indicators: dict[int, dict[str, str]]
) -> dict[str, Any]:
    """Normalize the independently verified 1990-2026 JSON format."""
    year = int(record["anio"])
    indicator = indicators.get(year, {})
    deflator_to_1986 = optional_float(indicator.get("deflactor_hasta_1986"))
    deflator_to_1990 = (
        deflator_to_1986 / DEFLATOR_1990_TO_1986
        if deflator_to_1986 is not None
        else None
    )
    gdp_pc_nominal = optional_float(indicator.get("pib_per_capita_eur_corriente"))
    original_currency = "pesetas" if record["moneda_original"] == "ESP" else "euros"
    brackets: list[dict[str, Any]] = []

    for source_bracket in record["tramos"]:
        index = int(source_bracket["numero_tramo"])
        lower_original = float(source_bracket["limite_inferior"])
        upper_original = optional_float(source_bracket.get("limite_superior"))
        lower_nominal = optional_float(source_bracket.get("limite_inferior_eur"))
        upper_nominal = optional_float(source_bracket.get("limite_superior_eur"))
        is_open = bool(source_bracket.get("abierto_por_arriba")) or upper_nominal is None
        brackets.append(
            {
                "index": index,
                "rate": float(source_bracket["tipo_marginal_pct"]),
                "lower_original": lower_original,
                "upper_original": upper_original,
                "lower_nominal_eur": lower_nominal,
                "upper_nominal_eur": None if is_open else upper_nominal,
                "lower_real_eur_1990": (
                    lower_nominal * deflator_to_1990
                    if lower_nominal is not None and deflator_to_1990 is not None
                    else None
                ),
                "upper_real_eur_1990": (
                    upper_nominal * deflator_to_1990
                    if upper_nominal is not None and deflator_to_1990 is not None and not is_open
                    else None
                ),
                "is_open": is_open,
            }
        )

    finite_limits = [
        bracket["upper_nominal_eur"]
        for bracket in brackets
        if bracket["upper_nominal_eur"] is not None
    ]
    if any(right <= left for left, right in zip(finite_limits, finite_limits[1:])):
        raise ValueError(f"{year}: verified finite upper limits are not increasing")
    if any(not 0 <= bracket["rate"] <= 100 for bracket in brackets):
        raise ValueError(f"{year}: verified marginal rate is outside 0-100")

    source_url = record["fuentes"][0]["url"]
    return {
        "year": year,
        "regime": record["ambito_legal"],
        "deflator_hasta_1986": deflator_to_1986,
        "deflator_hasta_1990": deflator_to_1990,
        "original_currency": original_currency,
        "source_url": source_url,
        "row_count": len(brackets),
        "finite_bracket_count": len(finite_limits),
        "gdp_pc_nominal_eur": gdp_pc_nominal,
        "gdp_pc_real_eur_1990": (
            gdp_pc_nominal * deflator_to_1990
            if gdp_pc_nominal is not None and deflator_to_1990 is not None
            else None
        ),
        "brackets": brackets,
    }


def validate_verified_csv(records: list[dict[str, Any]]) -> None:
    with VERIFIED_CSV_PATH.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter=";"))
    principal_rows = [row for row in rows if row.get("tipo_fila") == "tramo_principal"]
    expected = {
        (record["year"], bracket["index"]): bracket
        for record in records
        for bracket in record["brackets"]
    }
    if len(principal_rows) != len(expected):
        raise ValueError("Verified CSV does not contain exactly the JSON principal tramos")
    for row in principal_rows:
        key = (int(row["anio"]), int(row["numero_tramo"]))
        bracket = expected.get(key)
        if bracket is None:
            raise ValueError(f"Verified CSV has an unexpected tramo: {key}")
        csv_rate = float(row["tipo_marginal_pct"])
        csv_upper = optional_float(row["limite_superior_eur"])
        if csv_rate != bracket["rate"] or csv_upper != bracket["upper_nominal_eur"]:
            raise ValueError(f"Verified CSV disagrees with JSON at {key}")


def build_datasets() -> dict[str, list[dict[str, Any]]]:
    raw_original = load_json(RAW_ORIGINAL_PATH)
    audited_master = load_json(AUDITED_MASTER_PATH)
    indicators = load_indicators()
    original_years = {int(record["year"]) for record in raw_original["years"]}
    complete = [
        normalize_record(record, indicators) for record in audited_master["years"]
    ]
    original = [record for record in complete if record["year"] in original_years]
    if len(original) != len(original_years):
        raise ValueError("The audited master does not cover every original-sequence year")
    verified_source = load_json(VERIFIED_JSON_PATH)
    verified = [
        normalize_verified_record(record, indicators)
        for record in verified_source["anios"]
    ]
    validate_verified_csv(verified)
    if len(verified) != 37 or [record["year"] for record in verified] != list(range(1990, 2027)):
        raise ValueError("Verified sequence must cover every year from 1990 through 2026")
    return {"original": original, "complete": complete, "verified2026": verified}


def write_output(datasets: dict[str, list[dict[str, Any]]]) -> None:
    payload = json.dumps(datasets, ensure_ascii=False, separators=(",", ":"))
    content = (
        "// Generated by scripts/generate_irpf_chart_datasets.py from audited IRPF sources.\n"
        f"window.IRPF_CHART_DATASETS={payload};\n"
    )
    OUTPUT_PATH.write_text(content, encoding="utf-8")


def main() -> None:
    datasets = build_datasets()
    write_output(datasets)
    print(
        f"Generated {OUTPUT_PATH.name}: "
        f"{len(datasets['original'])} original years, "
        f"{len(datasets['complete'])} complete years, "
        f"{len(datasets['verified2026'])} verified 1990-2026 years"
    )


if __name__ == "__main__":
    main()
