#!/usr/bin/env python3
"""Validate the canonical IRPF XML and its fiscal arithmetic."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from xml.etree import ElementTree as ET


IRPF_DIR = Path(__file__).resolve().parents[1]
XML_PATH = IRPF_DIR / "irpf_escala_estatal_1990-2025.xml"
PESETAS_PER_EURO = Decimal("166.386")


def decimal(element: ET.Element, attribute: str) -> Decimal | None:
    value = element.get(attribute)
    return Decimal(value) if value is not None else None


def brackets(year: ET.Element) -> list[ET.Element]:
    container = year.find("./brackets")
    assert container is not None, f"{year.get('value')}: missing brackets"
    rows = container.findall("./bracket")
    assert int(container.get("count", "-1")) == len(rows)
    return rows


def assert_close(actual: Decimal | None, expected: Decimal, tolerance: Decimal, message: str) -> None:
    assert actual is not None and abs(actual - expected) <= tolerance, (
        f"{message}: expected {expected}, found {actual}"
    )


def validate_year(year: ET.Element) -> None:
    value = year.get("value")
    rows = brackets(year)
    assert rows, f"{value}: empty scale"
    assert [int(row.get("index", "0")) for row in rows] == list(range(1, len(rows) + 1))
    assert rows[-1].get("openUpper") == "true", f"{value}: final bracket must be open"

    for index, row in enumerate(rows):
        lower = decimal(row, "lowerOriginal")
        upper = decimal(row, "upperOriginal")
        rate = decimal(row, "marginalRatePct")
        assert lower is not None and lower >= 0
        assert rate is not None and Decimal("0") <= rate <= Decimal("100")
        is_open = row.get("openUpper") == "true"
        assert is_open == (upper is None), f"{value} T{index + 1}: inconsistent open limit"
        if upper is not None:
            assert upper > lower, f"{value} T{index + 1}: non-positive width"

        if index:
            previous = rows[index - 1]
            assert decimal(previous, "upperOriginal") == lower, (
                f"{value} T{index + 1}: discontinuous legal limits"
            )
            previous_quota = decimal(previous, "quotaAtLowerOriginal")
            current_quota = decimal(row, "quotaAtLowerOriginal")
            previous_lower = decimal(previous, "lowerOriginal")
            previous_upper = decimal(previous, "upperOriginal")
            previous_rate = decimal(previous, "marginalRatePct")
            assert None not in (previous_quota, previous_lower, previous_upper, previous_rate)
            expected_quota = previous_quota + (previous_upper - previous_lower) * previous_rate / 100
            tolerance = Decimal("1") if year.get("originalCurrency") == "ESP" else Decimal("0.02")
            assert_close(current_quota, expected_quota, tolerance, f"{value} T{index + 1} quota")

        nominal_pairs = (
            ("lowerOriginal", "lowerNominalEur"),
            ("upperOriginal", "upperNominalEur"),
            ("quotaAtLowerOriginal", "quotaAtLowerEur"),
        )
        for original_name, nominal_name in nominal_pairs:
            original = decimal(row, original_name)
            nominal = decimal(row, nominal_name)
            if original is None:
                assert nominal is None
                continue
            expected_nominal = (
                original / PESETAS_PER_EURO
                if year.get("originalCurrency") == "ESP"
                else original
            )
            assert_close(nominal, expected_nominal, Decimal("0.006"), f"{value} T{index + 1} {nominal_name}")

        deflator = decimal(year, "deflatorTo1990")
        if deflator is not None:
            for nominal_name, real_name in (
                ("lowerNominalEur", "lowerRealEur1990"),
                ("upperNominalEur", "upperRealEur1990"),
            ):
                nominal = decimal(row, nominal_name)
                real = decimal(row, real_name)
                if nominal is None:
                    assert real is None
                else:
                    assert_close(real, nominal * deflator, Decimal("0.00001"), f"{value} T{index + 1} {real_name}")


def scale_signature(year: ET.Element) -> list[tuple[Decimal, Decimal | None, Decimal]]:
    return [
        (
            decimal(row, "lowerOriginal"),
            decimal(row, "upperOriginal"),
            decimal(row, "marginalRatePct"),
        )
        for row in brackets(year)
    ]


def main() -> None:
    root = ET.parse(XML_PATH).getroot()
    assert root.tag == "irpfDataset"
    assert root.get("schemaVersion") == "1.0.0"
    assert root.get("cutoffDate") == "2026-07-29"
    assert root.get("endYear") == "2025"
    assert root.get("scope") == "state-general-income-tax-scale"

    years = root.findall("./years/year")
    expected_years = list(range(1990, 2026))
    assert [int(year.get("value", "0")) for year in years] == expected_years
    assert int(root.get("yearCount", "0")) == len(years) == 36
    assert root.findtext("./metadata/title", "").endswith("1990-2025")
    for year in years:
        validate_year(year)

    by_year = {int(year.get("value", "0")): year for year in years}
    assert all(by_year[year].get("legalScope") == "escala_general_nacional_unificada" for year in range(1990, 1997))
    assert all(by_year[year].get("legalScope") == "escala_estatal_general" for year in range(1997, 2026))

    expected_2005 = [
        (Decimal("0"), Decimal("4080"), Decimal("9.06")),
        (Decimal("4080"), Decimal("14076"), Decimal("15.84")),
        (Decimal("14076"), Decimal("26316"), Decimal("18.68")),
        (Decimal("26316"), Decimal("45900"), Decimal("24.71")),
        (Decimal("45900"), None, Decimal("29.16")),
    ]
    assert scale_signature(by_year[2005]) == expected_2005

    expected_2007 = [
        (Decimal("0"), Decimal("17360"), Decimal("15.66")),
        (Decimal("17360"), Decimal("32360"), Decimal("18.27")),
        (Decimal("32360"), Decimal("52360"), Decimal("24.14")),
        (Decimal("52360"), None, Decimal("27.13")),
    ]
    assert scale_signature(by_year[2007]) == expected_2007

    combined_rates = [Decimal(value) for value in ("12.75", "16", "21.5", "25.5", "27.5", "29.5", "30.5")]
    for value in range(2012, 2015):
        year = by_year[value]
        assert year.get("sourceStatus") == "combinacion_de_dos_escalas_estatales_oficiales"
        assert [decimal(row, "marginalRatePct") for row in brackets(year)] == combined_rates
        assert len(year.findall("./stateComponents/component")) == 2

    expected_current = [
        (Decimal("0"), Decimal("12450"), Decimal("9.5")),
        (Decimal("12450"), Decimal("20200"), Decimal("12")),
        (Decimal("20200"), Decimal("35200"), Decimal("15")),
        (Decimal("35200"), Decimal("60000"), Decimal("18.5")),
        (Decimal("60000"), Decimal("300000"), Decimal("22.5")),
        (Decimal("300000"), None, Decimal("24.5")),
    ]
    assert scale_signature(by_year[2025]) == expected_current
    assert by_year[2025].get("exerciseStatus") == "cerrado"
    expected_gdp = {
        2025: (Decimal("34213.53"), "estimacion_ameco_primavera_2026"),
    }
    for value, (nominal, status) in expected_gdp.items():
        year = by_year[value]
        assert decimal(year, "gdpPerCapitaNominalEur") == nominal
        assert decimal(year, "gdpPerCapitaRealEur1990") == nominal * decimal(year, "deflatorTo1990")
        assert year.get("gdpStatus") == status
        assert any("AMECO" in source.get("reference", "") for source in year.findall("./sources/source"))

    rates = [decimal(row, "marginalRatePct") for year in years for row in brackets(year)]
    assert Decimal("4400") not in rates
    assert min(rates) == Decimal("0") and max(rates) == Decimal("56")
    print("IRPF XML validation passed (36 years; structure, arithmetic and historical corrections OK).")


if __name__ == "__main__":
    main()
