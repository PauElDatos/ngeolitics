#!/usr/bin/env python3
"""Validate the tax-scale invariants used by the IRPF chart."""

from __future__ import annotations

import json
from pathlib import Path

from generate_irpf_chart_datasets import (
    AUDITED_MASTER_PATH,
    OUTPUT_PATH,
    build_datasets,
)


EXPECTED_2007_SOURCE = "https://www.boe.es/buscar/doc.php?id=BOE-A-2006-20764"


def bracket(record: dict, index: int) -> dict | None:
    return next((item for item in record["brackets"] if item["index"] == index), None)


def limit(record: dict, index: int) -> float | None:
    item = bracket(record, index)
    return None if item is None or item["is_open"] else item["upper_nominal_eur"]


def generated_payload(path: Path = OUTPUT_PATH) -> dict:
    source = path.read_text(encoding="utf-8")
    payload = source.split("=", 1)[1].rsplit(";", 1)[0]
    return json.loads(payload)


def validate_sequence(name: str, records: list[dict]) -> None:
    for record in records:
        finite = [
            item["upper_nominal_eur"]
            for item in record["brackets"]
            if item["upper_nominal_eur"] is not None
        ]
        assert all(left < right for left, right in zip(finite, finite[1:])), (
            f"{name} {record['year']}: unordered finite limits"
        )
        t3, t4 = limit(record, 3), limit(record, 4)
        assert t4 is None or t3 is None or t4 > t3, (
            f"{name} {record['year']}: T4 is not above T3"
        )


def main() -> None:
    datasets = build_datasets()
    generated = generated_payload()
    assert generated == datasets, "Generated browser asset is stale; run the generator"
    assert len(datasets["original"]) == 26
    assert len(datasets["complete"]) == 34

    for name, records in datasets.items():
        validate_sequence(name, records)
        by_year = {record["year"]: record for record in records}
        record_2007 = by_year[2007]
        finite_2007 = [
            item["upper_nominal_eur"]
            for item in record_2007["brackets"]
            if item["upper_nominal_eur"] is not None
        ]
        assert finite_2007 == [17360.0, 32360.0, 52360.0]
        assert [limit(by_year[year], 4) for year in range(2007, 2011)] == [None] * 4
        for year in list(range(2003, 2011)) + list(range(2015, 2021)):
            if year in by_year:
                assert limit(by_year[year], 5) is None, f"{name} {year}: T5 must be open/absent"
        t4_2007 = bracket(record_2007, 4)
        assert t4_2007 and t4_2007["is_open"] and t4_2007["rate"] == 27.13
        assert bracket(record_2007, 5) is None
        assert record_2007["source_url"] == EXPECTED_2007_SOURCE
        rates = [item["rate"] for record in records for item in record["brackets"]]
        assert 4400 not in rates
        assert max(rates) == 56.0

    master = json.loads(AUDITED_MASTER_PATH.read_text(encoding="utf-8"))
    master_2007 = next(record for record in master["years"] if record["year"] == 2007)
    assert master_2007["source_url"] == EXPECTED_2007_SOURCE
    print("IRPF dataset validation passed (10 data/model checks).")


if __name__ == "__main__":
    main()
