#!/usr/bin/env python3
"""Exercise the IRPF page in Chromium at desktop and mobile sizes."""

from __future__ import annotations

import functools
import http.server
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


def check_sequence(page: Page, sequence: str) -> None:
    page.evaluate("sequence => window.IRPF_TEST_API.setDataSequence(sequence)", sequence)
    page.wait_for_timeout(50)

    model = page.evaluate(
        """
        () => ({
          t4: window.IRPF_TEST_API.getLimitSeries(4),
          t5: window.IRPF_TEST_API.getLimitSeries(5),
          rate4: window.IRPF_TEST_API.getRateSeries(4),
          extent: window.IRPF_TEST_API.getRateExtent(),
        })
        """
    )
    t4_by_year = {point["year"]: point for point in model["t4"]}
    t5_by_year = {point["year"]: point for point in model["t5"]}
    rate4_by_year = {point["year"]: point for point in model["rate4"]}
    assert all(t4_by_year[year]["value"] is None for year in range(2007, 2011))
    for year in list(range(2003, 2011)) + list(range(2015, 2021)):
        if year in t5_by_year:
            assert t5_by_year[year]["value"] is None
    assert rate4_by_year[2007]["exists"]
    assert rate4_by_year[2007]["rate"] == 27.13
    assert model["extent"] == {"min": 0, "max": 56}

    assert page.locator('.rateHeatCell[data-year="2007"][data-tramo="4"]').count() == 1
    assert page.locator('.rateHeatCell[data-year="2007"][data-tramo="5"]').count() == 0
    assert page.locator(".rateHeatCell").count() > 0

    t4_path = page.locator('.limitSeries[data-tramo="4"]').get_attribute("d") or ""
    t5_path = page.locator('.limitSeries[data-tramo="5"]').get_attribute("d") or ""
    assert t4_path.count("M") >= 2, f"{sequence}: T4 path was not split"
    assert t5_path.count("M") >= 3, f"{sequence}: T5 path was not split twice"

    tooltip = normalized_text(
        page.evaluate("() => window.IRPF_TEST_API.showLimitsTooltip(2007)")
    )
    assert tooltip.count("Tramo ") == 4
    assert "Tramo 4 · desde 52.360 € · tipo marginal estatal 27,13 %" in tooltip
    assert "Tramo 5" not in tooltip


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
            heatCells: document.querySelectorAll('.rateHeatCell').length,
          };
        }
        """
    )
    assert metrics["overflow"] <= 1, metrics
    assert metrics["chartWidth"] > 250 and metrics["chartHeight"] > 300, metrics
    assert metrics["svgWidth"] > 250 and metrics["heatCells"] > 0, metrics


def check_all_visual_modes(page: Page) -> None:
    page.locator('#modeButtons button[data-key="real1990"]').click()
    page.wait_for_timeout(50)
    assert page.locator(".rateHeatCell").count() > 0
    assert page.locator('.limitSeries[data-tramo="4"]').count() == 1

    page.locator('#scaleButtons button[data-key="linear"]').click()
    page.wait_for_timeout(50)
    assert page.locator(".rateHeatCell").count() > 0
    page.locator('#scaleButtons button[data-key="log"]').click()

    page.locator("#btnChartPopulation").click()
    page.wait_for_timeout(50)
    assert "población adulta" in (page.locator("#chart").get_attribute("aria-label") or "")
    assert page.locator("#chart").bounding_box()["width"] > 250

    page.locator("#btnChartLimits").click()
    page.locator('#modeButtons button[data-key="nominal"]').click()
    page.wait_for_timeout(50)
    assert page.locator(".rateHeatCell").count() > 0


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
            desktop.locator('.limitSeries[data-tramo="4"]').wait_for()
            assert "Escala estatal del IRPF" in desktop.locator("h1").inner_text()
            check_sequence(desktop, "original")
            check_sequence(desktop, "complete")
            check_sequence(desktop, "verified2026")
            check_all_visual_modes(desktop)
            check_layout(desktop)
            desktop.locator("#chartBox").dispatch_event("mouseleave")
            desktop.screenshot(
                path=str(ARTIFACT_DIR / "desktop-complete.png"), full_page=True
            )

            mobile = browser.new_page(viewport={"width": 390, "height": 844})
            mobile.on("pageerror", lambda error: errors.append(str(error)))
            mobile.goto(url, wait_until="networkidle")
            mobile.locator('.limitSeries[data-tramo="4"]').wait_for()
            check_sequence(mobile, "original")
            check_sequence(mobile, "complete")
            check_sequence(mobile, "verified2026")
            check_all_visual_modes(mobile)
            check_layout(mobile)
            mobile.locator("#chartBox").dispatch_event("mouseleave")
            mobile.screenshot(
                path=str(ARTIFACT_DIR / "mobile-complete.png"), full_page=True
            )
            browser.close()
    finally:
        server.shutdown()
        server.server_close()

    assert not errors, f"Browser errors: {errors}"
    print(f"IRPF browser validation passed. Screenshots: {ARTIFACT_DIR}")


if __name__ == "__main__":
    main()
