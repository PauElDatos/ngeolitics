#!/usr/bin/env python3
"""Valida continuidad y puntos de control del modelo IRPF total con media autonómica."""
from __future__ import annotations
import json, re
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def load_js_array(path: Path, var: str):
    text = path.read_text(encoding="utf-8")
    match = re.search(rf"window\.{re.escape(var)}\s*=\s*(\[.*?\]);", text, re.S)
    if not match:
        raise AssertionError(f"No se encontró {var} en {path.name}")
    return json.loads(match.group(1))

def state_rates(year: int):
    root = ET.parse(ROOT / "irpf_escala_estatal_1990-2025.xml").getroot()
    node = root.find(f".//years/year[@value='{year}']")
    if node is None:
        raise AssertionError(f"Falta año estatal {year}")
    return [float(b.get("marginalRatePct")) for b in node.find("brackets").findall("bracket")]

def main():
    auto = load_js_array(ROOT / "irpf_autonomico_valencia_1997-2025.js", "IRPF_VALENCIA_AUTONOMIC_SCHEDULES")
    years = [row["year"] for row in auto]
    assert years == list(range(1997, 2026)), years

    by_year = {row["year"]: row for row in auto}
    assert state_rates(2010) == [12.0, 14.0, 18.5, 21.5]
    assert [b["rate"] for b in by_year[2010]["brackets"]] == [11.9, 13.92, 18.45, 21.48]
    assert abs(state_rates(2010)[-1] + by_year[2010]["brackets"][-1]["rate"] - 42.98) < 1e-9
    assert abs(state_rates(2025)[-1] + by_year[2025]["brackets"][-1]["rate"] - 54.0) < 1e-9
    assert [b["rate"] for b in by_year[2022]["brackets"]][-1] == 29.5
    assert [b["rate"] for b in by_year[2023]["brackets"]][:3] == [9.0, 12.0, 15.0]

    html = (ROOT / "index.html").read_text(encoding="utf-8")
    assert "effortScope: 'totalCcaa'" in html
    assert "autonomousPersonalMinimumStandard" in html
    assert "averageAutonomousIrpf" in html
    assert "autonomousScheduleCount" in html
    assert "return year >= 2022 ? 6105" in html
    assert "Total · media CCAA" in html
    assert "data_fuentes/IRPF_tablas_1990-2023.json" in html
    assert (ROOT / "data_fuentes" / "raw_irpf.rtf").exists()
    assert (ROOT / "data_fuentes" / "IRPF_tablas_1990-2023.json").exists()

    print("IRPF total media autonómica validation passed (1997–2025; 2010 transition and 2022+ minimum checks OK).")

if __name__ == "__main__":
    main()
