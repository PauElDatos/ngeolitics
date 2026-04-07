#!/usr/bin/env python3
"""
Scrapea las tablas del IRPF general estatal y autonómico desde páginas de la AEAT
para los ejercicios 2017, 2018, 2021, 2022 y 2023, y genera una salida en texto
con HTML normalizado estilo BOE.

Uso:
    python irpf_aeat_scraper.py --out irpf_tablas.txt
    python irpf_aeat_scraper.py --years 2023 2022 --out salida.txt

Dependencias:
    pip install requests beautifulsoup4
"""

from __future__ import annotations

import argparse
import html
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag, UnicodeDammit


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}

TIMEOUT = 30
SLEEP_SECONDS = 0.25
MAX_RETRIES = 3

YEAR_CONFIG = {
    2023: {
        "state": "https://sede.agenciatributaria.gob.es/Sede/ayuda/manuales-videos-folletos/manuales-practicos/irpf-2023/c15-calculo-impuesto-determinacion-cuotas-integras/gravamen-base-liquidable-general/gravamen-estatal.html",
        "autonomic_index": "https://sede.agenciatributaria.gob.es/Sede/ayuda/manuales-videos-folletos/manuales-practicos/irpf-2023/c15-calculo-impuesto-determinacion-cuotas-integras/gravamen-base-liquidable-general/gravamen-autonomico.html",
        "mode": "index_per_community",
    },
    2022: {
        "state": "https://sede.agenciatributaria.gob.es/Sede/ayuda/manuales-videos-folletos/manuales-practicos/irpf-2022/c15-calculo-impuesto-determinacion-cuotas-integras/gravamen-base-liquidable-general/gravamen-estatal.html",
        "autonomic_index": "https://sede.agenciatributaria.gob.es/Sede/ayuda/manuales-videos-folletos/manuales-practicos/irpf-2022/c15-calculo-impuesto-determinacion-cuotas-integras/gravamen-base-liquidable-general/gravamen-autonomico.html",
        "mode": "index_per_community",
    },
    2021: {
        "state": "https://sede.agenciatributaria.gob.es/Sede/ayuda/manuales-videos-folletos/manuales-practicos/irpf-2021/capitulo-15-calculo-impuesto-determinacion-integras/gravamen-base-liquidable-general/gravamen-estatal.html",
        "autonomic_index": "https://sede.agenciatributaria.gob.es/Sede/ayuda/manuales-videos-folletos/manuales-practicos/irpf-2021/capitulo-15-calculo-impuesto-determinacion-integras/gravamen-base-liquidable-general/gravamen-autonomico.html",
        "mode": "index_per_community",
    },
    2018: {
        "state": "https://sede.agenciatributaria.gob.es/Sede/manuales/ejercicio-2018/modelo-100/modelo-100/8-cumplimentacion-irpf/8_6-cuota-integra/8_6_1-gravamen-base-liquidable-general/8_6_1_1-cuota-integra-estatal.html",
        "autonomic_all": "https://sede.agenciatributaria.gob.es/Sede/manuales/ejercicio-2018/modelo-100/modelo-100/8-cumplimentacion-irpf/8_6-cuota-integra/8_6_1-gravamen-base-liquidable-general/8_6_1_2-cuota-integra-autonomica.html",
        "mode": "single_page_many_communities",
    },
    2017: {
        "state": "https://sede.agenciatributaria.gob.es/Sede/manuales/ejercicio-2017/modelo-100/8-cumplimentacion-irpf/8_6-cuota-integra/8_6_1-gravamen-base-liquidable-general/8_6_1_1-cuota-integra-estatal.html",
        "autonomic_all": "https://sede.agenciatributaria.gob.es/Sede/manuales/ejercicio-2017/modelo-100/8-cumplimentacion-irpf/8_6-cuota-integra/8_6_1-gravamen-base-liquidable-general/8_6_1_2-cuota-integra-autonomica.html",
        "mode": "single_page_many_communities",
    },
}

TABLE_HEADERS = [
    ["Base liquidable", "–", "Hasta euros"],
    ["Cuota íntegra", "–", "Euros"],
    ["Resto base liquidable", "–", "Hasta euros"],
    ["Tipo aplicable", "–", "Porcentaje"],
]

COMMUNITY_PATTERNS = [
    r"^Comunidad Autónoma",
    r"^Comunidad de ",
    r"^Comunitat Valenciana",
    r"^Ciudad con Estatuto",
    r"^Especialidad:",
]

ROW_RE = re.compile(
    r"^\s*"
    r"(?P<base>(?:\d|\.|,)+(?:,\d+)?|0(?:,00)?|en\s+adelante|En\s+adelante)\s+"
    r"(?P<cuota>(?:\d|\.|,)+(?:,\d+)?|0(?:,00)?)\s+"
    r"(?P<resto>(?:\d|\.|,)+(?:,\d+)?|En\s+adelante|en\s+adelante)\s+"
    r"(?P<tipo>(?:\d|\.|,)+(?:,\d+)?%?)\s*$",
    re.IGNORECASE,
)

TRAILING_ROW_RE = re.compile(
    r"(0(?:,00)?|\d[\d\.,]*)\s+"
    r"(0(?:,00)?|\d[\d\.,]*)\s+"
    r"(En\s+adelante|en\s+adelante|\d[\d\.,]*)\s+"
    r"(\d[\d\.,]*%?)$",
    re.IGNORECASE,
)


@dataclass
class TableBlock:
    year: int
    scope: str
    label: str
    url: str
    rows: List[List[str]]


def repair_mojibake(text: str) -> str:
    if not text:
        return text

    original = text
    bad_markers = ("Ã", "Â", "â€", "â€“", "â€”", "â€œ", "â€\x9d", "â€\x99")
    if any(marker in text for marker in bad_markers):
        candidates = []
        for src in ("latin-1", "cp1252"):
            try:
                fixed = text.encode(src).decode("utf-8")
                candidates.append(fixed)
            except (UnicodeEncodeError, UnicodeDecodeError):
                pass
        if candidates:
            def score(s: str) -> tuple[int, int]:
                penalty = sum(s.count(m) for m in bad_markers)
                bonus = sum(s.count(ch) for ch in "áéíóúÁÉÍÓÚñÑüÜ")
                return (penalty, -bonus)
            text = min(candidates, key=score)

    return unicodedata.normalize("NFC", text if text else original)


def normalize_space(text: str) -> str:
    text = repair_mojibake(text)
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def request_html(session: requests.Session, url: str) -> str:
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=TIMEOUT)
            resp.raise_for_status()
            dammit = UnicodeDammit(resp.content, is_html=True)
            text = dammit.unicode_markup
            if text is None:
                text = resp.content.decode(resp.apparent_encoding or "utf-8", errors="replace")
            text = repair_mojibake(text)
            time.sleep(SLEEP_SECONDS)
            return text
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < MAX_RETRIES:
                time.sleep(0.5 * attempt)
                continue
            break
    raise RuntimeError(f"No pude descargar {url}: {last_exc}")


def soup_from_html(text: str) -> BeautifulSoup:
    return BeautifulSoup(text, "html.parser")


def table_rows_from_table_tag(table: Tag) -> List[List[str]]:
    rows: List[List[str]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        vals = [normalize_space(cell.get_text(" ", strip=True)) for cell in cells]
        if vals:
            rows.append(vals)
    return rows


def is_numericish(value: str) -> bool:
    v = normalize_space(value).lower()
    return bool(re.search(r"\d", v) or "adelante" in v or v.endswith("%"))


def is_candidate_table(rows: Sequence[Sequence[str]]) -> bool:
    if len(rows) < 2:
        return False
    joined = " ".join(" ".join(r) for r in rows[:4]).lower()
    if (
        "base liquidable" in joined
        and "cuota íntegra" in joined
        and ("tipo aplicable" in joined or "tipo" in joined)
    ):
        return True

    numeric_rows = 0
    for row in rows:
        if len(row) >= 4:
            tail = row[-4:]
            if sum(1 for cell in tail if is_numericish(cell)) >= 3:
                numeric_rows += 1
    return numeric_rows >= 2


def extract_html_tables(soup: BeautifulSoup) -> List[List[List[str]]]:
    out: List[List[List[str]]] = []
    for table in soup.find_all("table"):
        rows = table_rows_from_table_tag(table)
        if is_candidate_table(rows):
            out.append(rows)
    return out


def clean_cell(value: str) -> str:
    value = normalize_space(value)
    value = value.rstrip(".")
    return value


def normalize_rows(rows: List[List[str]]) -> List[List[str]]:
    normalized: List[List[str]] = []
    for row in rows:
        row = [clean_cell(x) for x in row if clean_cell(x)]
        if not row:
            continue
        joined = " ".join(row).lower()
        if "base liquidable" in joined and "cuota íntegra" in joined:
            continue
        if row[0].lower().startswith("escala aplicable"):
            m = TRAILING_ROW_RE.search(row[0])
            if m:
                normalized.append([clean_cell(m.group(i)) for i in range(1, 5)])
            elif len(row) >= 5:
                normalized.append([clean_cell(x) for x in row[-4:]])
            continue
        if len(row) >= 4:
            candidate = [clean_cell(x) for x in row[-4:]]
            if sum(1 for c in candidate if is_numericish(c)) >= 3:
                normalized.append(candidate)
                continue
        if len(row) == 1:
            m = ROW_RE.match(row[0])
            if m:
                normalized.append([
                    clean_cell(m.group("base")),
                    clean_cell(m.group("cuota")),
                    clean_cell(m.group("resto")),
                    clean_cell(m.group("tipo")),
                ])
    return normalized


def text_lines(soup: BeautifulSoup) -> List[str]:
    main = soup.select_one("main") or soup.select_one("article") or soup.body or soup
    lines = [normalize_space(x) for x in main.get_text("\n").splitlines()]
    return [x for x in lines if x]


def extract_single_table_from_text(soup: BeautifulSoup) -> List[List[str]]:
    lines = text_lines(soup)
    rows: List[List[str]] = []
    capture = False

    # Primer intento: desde la cabecera de tabla hacia abajo.
    for line in lines:
        low = line.lower()
        if "base liquidable" in low and "cuota íntegra" in low:
            capture = True
            continue
        if not capture:
            continue
        if low.startswith("tipo medio") or low.startswith("generar pdf"):
            break
        m = ROW_RE.match(line)
        if m:
            rows.append([
                clean_cell(m.group("base")),
                clean_cell(m.group("cuota")),
                clean_cell(m.group("resto")),
                clean_cell(m.group("tipo")),
            ])
            continue
        m2 = TRAILING_ROW_RE.search(line)
        if m2:
            rows.append([clean_cell(m2.group(i)) for i in range(1, 5)])

    if len(rows) >= 2:
        return rows

    # Segundo intento: barrido general por si la página vino sin la línea cabecera.
    rows = []
    for line in lines:
        m = ROW_RE.match(line)
        if m:
            rows.append([
                clean_cell(m.group("base")),
                clean_cell(m.group("cuota")),
                clean_cell(m.group("resto")),
                clean_cell(m.group("tipo")),
            ])
            continue
        m2 = TRAILING_ROW_RE.search(line)
        if m2 and ("escala aplicable" in line.lower() or len(rows) > 0):
            rows.append([clean_cell(m2.group(i)) for i in range(1, 5)])

    if len(rows) >= 2:
        return rows

    raise ValueError("No pude reconstruir la tabla única desde el texto de la página")


def extract_community_links(index_html: str, base_url: str, include_special: bool) -> List[tuple[str, str]]:
    soup = soup_from_html(index_html)
    candidates: List[tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        label = normalize_space(a.get_text(" ", strip=True))
        href = a["href"]
        if not href:
            continue
        if not any(re.search(pat, label, re.I) for pat in COMMUNITY_PATTERNS):
            continue
        if not include_special and label.lower().startswith("especialidad"):
            continue
        candidates.append((label, urljoin(base_url, href)))

    seen = set()
    unique: List[tuple[str, str]] = []
    for label, url in candidates:
        key = (label, url)
        if key in seen:
            continue
        seen.add(key)
        unique.append((label, url))
    return unique


def extract_blocks_from_single_table_page(
    session: requests.Session,
    year: int,
    url: str,
    label: str,
    scope: str,
) -> List[TableBlock]:
    html_text = request_html(session, url)
    soup = soup_from_html(html_text)

    try:
        tables = extract_html_tables(soup)
        if tables:
            best = max(tables, key=lambda t: len(normalize_rows(t)))
            rows = normalize_rows(best)
        else:
            rows = extract_single_table_from_text(soup)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Fallo extrayendo tabla en {year} | {label} | {url}: {exc}") from exc

    if len(rows) < 2:
        raise RuntimeError(f"Tabla vacía o incompleta en {year} | {label} | {url}")

    return [TableBlock(year=year, scope=scope, label=label, url=url, rows=rows)]


def is_heading_text(text: str) -> bool:
    text = normalize_space(text)
    return any(re.search(pat, text, re.I) for pat in COMMUNITY_PATTERNS)


def extract_blocks_from_multi_community_page(
    session: requests.Session,
    year: int,
    url: str,
    include_special: bool,
) -> List[TableBlock]:
    html_text = request_html(session, url)
    soup = soup_from_html(html_text)

    html_tables = extract_html_tables(soup)
    if html_tables:
        blocks: List[TableBlock] = []
        for table in soup.find_all("table"):
            rows = table_rows_from_table_tag(table)
            if not is_candidate_table(rows):
                continue
            heading = None
            prev = table
            while True:
                prev = prev.find_previous()
                if prev is None:
                    break
                if isinstance(prev, Tag):
                    txt = normalize_space(prev.get_text(" ", strip=True))
                    if is_heading_text(txt):
                        heading = txt.rstrip(":")
                        break
            if heading is None:
                heading = "Comunidad no identificada"
            if (not include_special) and heading.lower().startswith("especialidad"):
                continue
            parsed_rows = normalize_rows(rows)
            if len(parsed_rows) < 2:
                continue
            blocks.append(TableBlock(year=year, scope="autonómica", label=heading, url=url, rows=parsed_rows))
        if blocks:
            return dedupe_blocks(blocks)

    lines = text_lines(soup)
    blocks: List[TableBlock] = []
    current_label: Optional[str] = None
    current_rows: List[List[str]] = []

    def flush() -> None:
        nonlocal current_label, current_rows
        if current_label and len(current_rows) >= 2:
            if include_special or not current_label.lower().startswith("especialidad"):
                blocks.append(TableBlock(year=year, scope="autonómica", label=current_label, url=url, rows=current_rows))
        current_label = None
        current_rows = []

    for line in lines:
        if is_heading_text(line):
            flush()
            current_label = line.rstrip(":")
            continue
        if current_label is None:
            continue
        low = line.lower()
        if low.startswith("tipo medio") or low.startswith("generar pdf"):
            flush()
            break
        m = ROW_RE.match(line)
        if m:
            current_rows.append([
                clean_cell(m.group("base")),
                clean_cell(m.group("cuota")),
                clean_cell(m.group("resto")),
                clean_cell(m.group("tipo")),
            ])
            continue
        m2 = TRAILING_ROW_RE.search(line)
        if m2 and "escala aplicable" in low:
            current_rows.append([clean_cell(m2.group(i)) for i in range(1, 5)])
    flush()

    if not blocks:
        raise RuntimeError(f"No pude extraer tablas autonómicas de la página multi-comunidad: {url}")
    return dedupe_blocks(blocks)


def dedupe_blocks(blocks: Iterable[TableBlock]) -> List[TableBlock]:
    seen = set()
    out: List[TableBlock] = []
    for block in blocks:
        key = (block.label, tuple(tuple(r) for r in block.rows))
        if key in seen:
            continue
        seen.add(key)
        out.append(block)
    return out


def render_table_html(rows: Sequence[Sequence[str]]) -> str:
    out: List[str] = []
    out.append('<table class="tabla">')
    out.append("  <tbody>")
    out.append("    <tr>")
    for header_group in TABLE_HEADERS:
        out.append("      <th>")
        for part in header_group:
            out.append(f'        <p class="cabeza_tabla">{html.escape(part)}</p>')
        out.append("      </th>")
    out.append("    </tr>")

    for row in rows:
        if len(row) != 4:
            continue
        base, cuota, resto, tipo = [html.escape(clean_cell(c)) for c in row]
        out.append("    <tr>")
        out.append("      <td>")
        out.append(f'        <p class="cuerpo_tabla_der">{base}</p>')
        out.append("      </td>")
        out.append("      <td>")
        out.append(f'        <p class="cuerpo_tabla_der">{cuota}</p>')
        out.append("      </td>")
        out.append("      <td>")
        out.append(f'        <p class="cuerpo_tabla_centro">{resto}</p>')
        out.append("      </td>")
        out.append("      <td>")
        out.append(f'        <p class="cuerpo_tabla_centro">{tipo}</p>')
        out.append("      </td>")
        out.append("    </tr>")

    out.append("  </tbody>")
    out.append("</table>")
    return "\n".join(out)


def scrape_year(session: requests.Session, year: int, include_special: bool) -> List[TableBlock]:
    if year not in YEAR_CONFIG:
        raise KeyError(f"Año no configurado: {year}")
    cfg = YEAR_CONFIG[year]

    blocks: List[TableBlock] = []
    blocks.extend(
        extract_blocks_from_single_table_page(
            session=session,
            year=year,
            url=cfg["state"],
            label="Tramo estatal / nacional general",
            scope="estatal",
        )
    )

    mode = cfg["mode"]
    if mode == "index_per_community":
        index_html = request_html(session, cfg["autonomic_index"])
        links = extract_community_links(index_html, cfg["autonomic_index"], include_special)
        for label, url in links:
            blocks.extend(
                extract_blocks_from_single_table_page(
                    session=session,
                    year=year,
                    url=url,
                    label=label,
                    scope="autonómica",
                )
            )
    elif mode == "single_page_many_communities":
        blocks.extend(
            extract_blocks_from_multi_community_page(
                session=session,
                year=year,
                url=cfg["autonomic_all"],
                include_special=include_special,
            )
        )
    else:
        raise ValueError(f"Modo no soportado: {mode}")

    return blocks


def format_blocks(blocks: Sequence[TableBlock]) -> str:
    lines: List[str] = []
    current_year: Optional[int] = None
    for block in blocks:
        if current_year != block.year:
            if lines:
                lines.append("")
            lines.append(str(block.year))
            current_year = block.year
        lines.append(block.label)
        lines.append(block.url)
        lines.append(render_table_html(block.rows))
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scraper de tablas IRPF AEAT")
    parser.add_argument(
        "--years",
        nargs="*",
        type=int,
        default=sorted(YEAR_CONFIG.keys(), reverse=True),
        help="Años a descargar. Por defecto: 2023 2022 2021 2018 2017",
    )
    parser.add_argument(
        "--out",
        default="irpf_aeat_tablas.txt",
        help="Fichero de salida en texto",
    )
    parser.add_argument(
        "--include-special",
        action="store_true",
        help="Incluye también la especialidad de Ceuta/Melilla cuando exista",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    years = args.years

    invalid = [y for y in years if y not in YEAR_CONFIG]
    if invalid:
        print(
            f"Años no soportados en este script: {', '.join(map(str, invalid))}. "
            f"Soportados: {', '.join(map(str, sorted(YEAR_CONFIG)))}",
            file=sys.stderr,
        )
        return 2

    all_blocks: List[TableBlock] = []
    with requests.Session() as session:
        for year in years:
            print(f"[INFO] Descargando {year}...", file=sys.stderr)
            blocks = scrape_year(session, year, include_special=args.include_special)
            state = [b for b in blocks if b.scope == "estatal"]
            auto = sorted([b for b in blocks if b.scope == "autonómica"], key=lambda b: b.label.casefold())
            all_blocks.extend(state + auto)

    content = format_blocks(all_blocks)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"[OK] Salida escrita en: {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
