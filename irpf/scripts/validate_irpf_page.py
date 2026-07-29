#!/usr/bin/env python3
"""Exercise the IRPF page in Chromium at desktop and mobile sizes."""

from __future__ import annotations

import functools
import http.server
import math
import re
import tempfile
import threading
from pathlib import Path

from playwright.sync_api import Page, sync_playwright


REPO_ROOT = Path(__file__).resolve().parents[2]
ARTIFACT_DIR = Path(tempfile.gettempdir()) / "ngeolitics-irpf-validation"


def serve_repo() -> tuple[http.server.ThreadingHTTPServer, str]:
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler,
        directory=str(REPO_ROOT),
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    return server, f"http://{host}:{port}/irpf/index.html"


def normalized_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def check_dataset(page: Page) -> None:
    model = page.evaluate(
        """
        () => ({
          years: window.IRPF_TEST_API.getYears(),
          population: window.IRPF_TEST_API.getPopulationData(),
          t4: window.IRPF_TEST_API.getLimitSeries(4),
          t5: window.IRPF_TEST_API.getLimitSeries(5),
          rate4: window.IRPF_TEST_API.getRateSeries(4),
          extent: window.IRPF_TEST_API.getRateExtent(),
          rateRatios: [0, 9.5, 12, 28, 53.5, 56].map(rate => window.IRPF_TEST_API.getRateRatio(rate)),
          legendMidpointPosition: Number.parseFloat(document.querySelector('.rateLegendTick[data-rate="28"]').style.left),
        })
        """
    )
    t4_by_year = {point["year"]: point for point in model["t4"]}
    t5_by_year = {point["year"]: point for point in model["t5"]}
    rate4_by_year = {point["year"]: point for point in model["rate4"]}
    population_by_year = {point["year"]: point for point in model["population"]}
    assert model["years"] == list(range(1990, 2026))
    assert sorted(population_by_year) == list(range(1990, 2026))
    assert population_by_year[2025]["adultPopulation"] == 39265480
    assert population_by_year[2025]["thresholdStatus"] == "ameco_extrapolation"
    assert population_by_year[2025]["populationStatus"] == "wid"
    assert all(t4_by_year[year]["value"] is None for year in range(2007, 2011))
    for year in list(range(2003, 2011)) + list(range(2015, 2021)):
        if year in t5_by_year:
            assert t5_by_year[year]["value"] is None
    assert rate4_by_year[2007]["exists"]
    assert rate4_by_year[2007]["rate"] == 27.13
    assert model["extent"] == {"min": 0, "max": 56}
    expected_ratios = [math.log1p(rate) / math.log1p(56) for rate in (0, 9.5, 12, 28, 53.5, 56)]
    assert all(abs(actual - expected) < 1e-12 for actual, expected in zip(model["rateRatios"], expected_ratios))
    assert model["rateRatios"][2] - model["rateRatios"][1] > 4 * (
        model["rateRatios"][5] - model["rateRatios"][4]
    )
    assert abs(model["legendMidpointPosition"] - (model["rateRatios"][3] * 100)) < 1e-4
    assert page.locator('.rateLegendGradient[data-scale="logarithmic"]').count() == 1
    legend_label_gap = page.evaluate(
        """
        () => {
          const midpoint = document.querySelector('.rateLegendTick[data-rate="28"]').getBoundingClientRect();
          const maximum = document.querySelector('.rateLegendTick[data-rate="56"]').getBoundingClientRect();
          return maximum.left - midpoint.right;
        }
        """
    )
    assert legend_label_gap >= 2

    assert page.locator('.fiscalSurfaceCell[data-year="2007"][data-tramo="4"]').count() == 1
    assert page.locator('.fiscalSurfaceCell[data-year="2007"][data-tramo="5"]').count() == 0
    assert page.locator(".fiscalSurfaceCell").count() > 0
    assert page.locator('.fiscalSurfaceCell[data-year="2013"][data-tramo="7"]').count() == 1
    assert page.locator('.fiscalSurfaceCell[data-year="1995"][data-tramo="18"]').count() == 1
    surface_tramos = page.locator(".fiscalSurfaceCell").evaluate_all(
        "nodes => [...new Set(nodes.map(node => Number(node.dataset.tramo)))].sort((a, b) => a - b)"
    )
    assert surface_tramos == list(range(1, 19))
    assert page.locator(".limitSeries, .rateHeatCell, .openBracketPoint").count() == 0
    assert page.locator('.fiscalSurfaceCell[data-open="true"]').count() == 36
    annual_structure = page.evaluate(
        """
        () => [...new Set([...document.querySelectorAll('.fiscalSurfaceCell')].map(node => node.dataset.year))]
          .map(year => ({
            year: Number(year),
            cells: document.querySelectorAll(`.fiscalSurfaceCell[data-year="${year}"]`).length,
            boundaries: document.querySelectorAll(`.taxBoundary[data-year="${year}"]`).length,
            open: document.querySelectorAll(`.fiscalSurfaceCell[data-year="${year}"][data-open="true"]`).length,
          }))
        """
    )
    assert all(row["boundaries"] == row["cells"] - 1 for row in annual_structure)
    assert all(row["open"] == 1 for row in annual_structure)
    assert page.locator('.fiscalSurfaceCell[data-year="2008"]').count() == 4
    assert page.locator('.taxBoundary[data-year="2008"]').count() == 3
    open_2008 = page.locator('.fiscalSurfaceCell[data-year="2008"][data-tramo="4"]')
    assert open_2008.get_attribute("data-open") == "true"
    assert float(open_2008.get_attribute("data-lower")) == 53407.2
    adjacency_2008 = page.evaluate(
        """
        () => {
          const t3 = document.querySelector('.fiscalSurfaceCell[data-year="2008"][data-tramo="3"]');
          const t4 = document.querySelector('.fiscalSurfaceCell[data-year="2008"][data-tramo="4"]');
          const t3Top = Number(t3.getAttribute('y'));
          const t4Top = Number(t4.getAttribute('y'));
          const t4Bottom = t4Top + Number(t4.getAttribute('height'));
          return { t3Top, t4Top, t4Bottom };
        }
        """
    )
    assert adjacency_2008["t4Top"] < adjacency_2008["t3Top"]
    assert abs(adjacency_2008["t4Bottom"] - adjacency_2008["t3Top"]) < 0.01
    assert "Cada columna se calcula de forma independiente" in page.locator("#explanationContent").inner_text()
    assert page.locator("#countButtons").count() == 0

    tooltip = normalized_text(
        page.evaluate("() => window.IRPF_TEST_API.showLimitsTooltip(2007)")
    )
    assert tooltip.count("Tramo ") == 4
    assert "Tramo 4 · desde 52.360 € en adelante · tipo marginal estatal 27,13 %" in tooltip
    assert "Tramo 5" not in tooltip

    gdp_tooltip = normalized_text(
        page.evaluate("() => window.IRPF_TEST_API.showLimitsTooltip(2025)")
    )
    assert "PIB per cápita · estimación AMECO 34.214 €" in gdp_tooltip

    population_tooltip = normalized_text(
        page.evaluate("() => window.IRPF_TEST_API.showPopulationTooltip(2025)")
    )
    assert "Población adulta: 39.265.480" in population_tooltip
    assert "Umbrales extrapolados con el crecimiento del PIB per cápita AMECO" in population_tooltip
    assert "población WID" in population_tooltip
    page.evaluate("() => window.IRPF_TEST_API.showLimitsTooltip(2007)")


def check_layout(page: Page) -> None:
    metrics = page.evaluate(
        """
        () => {
          const chart = document.querySelector('#chartBox').getBoundingClientRect();
          return {
            overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
            chartWidth: chart.width,
            chartHeight: chart.height,
            svgWidth: document.querySelector('#chart').getBoundingClientRect().width,
            surfaceCells: document.querySelectorAll('.fiscalSurfaceCell').length,
          };
        }
        """
    )
    assert metrics["overflow"] <= 1, metrics
    assert metrics["chartWidth"] > 250 and metrics["chartHeight"] > 300, metrics
    assert metrics["svgWidth"] > 250 and metrics["surfaceCells"] > 0, metrics


def check_all_visual_modes(page: Page) -> None:
    page.locator('#modeButtons button[data-key="real1990"]').click()
    page.wait_for_timeout(50)
    assert page.locator(".fiscalSurfaceCell").count() > 0
    assert page.locator('.fiscalSurfaceCell[data-year="2008"][data-tramo="4"]').count() == 1

    page.locator('#scaleButtons button[data-key="linear"]').click()
    page.wait_for_timeout(50)
    assert page.locator(".fiscalSurfaceCell").count() > 0
    page.locator('#scaleButtons button[data-key="log"]').click()

    page.locator("#btnChartPopulation").click()
    page.wait_for_timeout(50)
    assert "población adulta" in (page.locator("#chart").get_attribute("aria-label") or "")
    assert page.locator("#chart").bounding_box()["width"] > 250

    page.locator("#btnChartLimits").click()
    page.locator('#modeButtons button[data-key="nominal"]').click()
    page.wait_for_timeout(50)
    assert page.locator(".fiscalSurfaceCell").count() > 0


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    server, url = serve_repo()
    errors: list[str] = []
    try:
        with sync_playwright() as playwright:
            try:
                browser = playwright.chromium.launch(channel="chrome", headless=True)
            except Exception:
                browser = playwright.chromium.launch(headless=True)

            desktop = browser.new_page(viewport={"width": 1440, "height": 1000})
            desktop.on("pageerror", lambda error: errors.append(str(error)))
            desktop.goto(url, wait_until="networkidle")
            desktop.locator('.fiscalSurfaceCell[data-year="2008"][data-tramo="4"]').wait_for()
            assert "Escala estatal del IRPF" in desktop.locator("h1").inner_text()
            assert desktop.get_by_text("Secuencia de datos", exact=True).count() == 0
            assert desktop.locator('a[href="irpf_escala_estatal_1990-2025.xml"]').count() == 1
            check_dataset(desktop)
            check_all_visual_modes(desktop)
            check_layout(desktop)
            desktop.locator("#chartBox").dispatch_event("mouseleave")
            desktop.screenshot(
                path=str(ARTIFACT_DIR / "desktop-2025.png"), full_page=True
            )

            mobile = browser.new_page(viewport={"width": 390, "height": 844})
            mobile.on("pageerror", lambda error: errors.append(str(error)))
            mobile.goto(url, wait_until="networkidle")
            mobile.locator('.fiscalSurfaceCell[data-year="2008"][data-tramo="4"]').wait_for()
            assert mobile.get_by_text("Secuencia de datos", exact=True).count() == 0
            check_dataset(mobile)
            check_all_visual_modes(mobile)
            check_layout(mobile)
            mobile.locator("#chartBox").dispatch_event("mouseleave")
            mobile.screenshot(
                path=str(ARTIFACT_DIR / "mobile-2025.png"), full_page=True
            )
            browser.close()
    finally:
        server.shutdown()
        server.server_close()

    assert not errors, f"Browser errors: {errors}"
    print(f"IRPF browser validation passed. Screenshots: {ARTIFACT_DIR}")


if __name__ == "__main__":
    main()
