#!/usr/bin/env python3
"""Orginfo.uz scraper.

Pipeline:
1) collect-links: iterate INN range, search, open first result, save company links.
2) parse-companies: open each company link and extract structured fields.

Usage examples:
    python DataBase/orginfo_parser.py collect-links --start-inn 100000001 --end-inn 100000100
    python DataBase/orginfo_parser.py parse-companies --links-csv data/orginfo_company_links.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
import random
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import Error, Page, TimeoutError, sync_playwright

BASE_URL = "https://orginfo.uz/uz/"
DEFAULT_LINKS_CSV = Path("data/orginfo_company_links.csv")
DEFAULT_COMPANIES_CSV = Path("data/orginfo_companies.csv")
COMPANY_COLUMNS = [
    "source_url",
    "company_name",
    "company_name_raw",
    "legal_form",
    "short_name",
    "inn",
    "registration_date",
    "activity_status",
    "registration_authority",
    "thsht",
    "dbibt",
    "ifut",
    "charter_capital_uzs",
    "email",
    "phone",
    "address",
    "region",
    "district",
    "category",
    "tax_committee",
    "large_taxpayer",
    "director",
    "founders",
    "employees_count",
    "branch_count",
]


@dataclass
class ScraperConfig:
    headless: bool
    timeout_ms: int
    min_delay: float
    max_delay: float
    base_url: str
    search_input_selector: str
    submit_selector: str
    result_link_selector: str
    debug_dir: Path


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def sleep_random(min_delay: float, max_delay: float) -> None:
    if max_delay <= 0:
        return
    delay = random.uniform(min_delay, max_delay)
    time.sleep(max(delay, 0))


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_existing_links(path: Path) -> Set[str]:
    if not path.exists():
        return set()

    links: Set[str] = set()
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            link = (row.get("company_url") or "").strip()
            if link:
                links.add(link)
    return links


def append_links_rows(path: Path, rows: Sequence[Dict[str, str]]) -> None:
    ensure_parent_dir(path)
    file_exists = path.exists()

    with path.open("a", newline="", encoding="utf-8") as f:
        fieldnames = ["inn", "company_url", "captured_at"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def write_company_rows(path: Path, rows: Sequence[Dict[str, str]]) -> None:
    ensure_parent_dir(path)
    if not rows:
        logging.warning("No company rows to write")
        return

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COMPANY_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "-") for col in COMPANY_COLUMNS})


def to_absolute(base_url: str, href: str) -> str:
    return urljoin(base_url, href.strip())


def is_company_like_url(url: str) -> bool:
    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    if not parsed.netloc.endswith("orginfo.uz"):
        return False

    blocked_prefixes = (
        "/uz/",
        "/uz/search/",
        "/ru/",
        "/en/",
        "/api/",
        "/i18n/",
        "/static/",
    )
    if path in blocked_prefixes:
        return False

    if path.startswith("/uz/search/") or path.startswith("/api/"):
        return False

    # Keep only likely company detail pages.
    return any(
        token in path
        for token in (
            "company",
            "companies",
            "comp",
            "firm",
            "organization",
            "organizats",
            "reestr",
            "details",
            "card",
            "profile",
            "entity",
            "yuridik",
        )
    )


def find_search_input(page: Page, custom_selector: str):
    if custom_selector:
        locator = page.locator(custom_selector)
        if locator.count() > 0:
            return locator.first
        raise RuntimeError(f"custom search input selector not found: {custom_selector}")

    selectors = [
        "input[name*='inn' i]",
        "input[placeholder*='инн' i]",
        "input[placeholder*='STIR' i]",
        "input[placeholder*='stir' i]",
        "input[placeholder*='tin' i]",
        "input[placeholder*='inn' i]",
        "input[id*='inn' i]",
        "input[type='search']",
        "form input[name='q']",
        "input[name*='search' i]",
        "input[id*='search' i]",
        "form input[type='text']",
    ]

    for selector in selectors:
        locator = page.locator(selector)
        if locator.count() > 0:
            return locator.first
    return None


def click_search(page: Page, custom_selector: str) -> None:
    if custom_selector:
        locator = page.locator(custom_selector)
        if locator.count() > 0:
            locator.first.click()
            return
        raise RuntimeError(f"custom submit selector not found: {custom_selector}")

    selectors = [
        "button[type='submit']",
        "button:has-text('Izlash')",
        "button:has-text('Поиск')",
        "button:has-text('Qidirish')",
        "button:has-text('Найти')",
        "a:has-text('Izlash')",
        "input[type='submit']",
    ]

    for selector in selectors:
        locator = page.locator(selector)
        if locator.count() > 0:
            locator.first.click()
            return

    page.keyboard.press("Enter")


def extract_first_result_link(page: Page, custom_selector: str) -> Optional[str]:
    if custom_selector:
        anchors = page.locator(custom_selector)
        count = min(anchors.count(), 120)
        for idx in range(count):
            href = anchors.nth(idx).get_attribute("href")
            if not href:
                continue
            absolute = to_absolute(page.url, href)
            if is_company_like_url(absolute):
                return absolute
        return None

    candidate_selectors = [
        "main a[href]",
        "section a[href]",
        "table a[href]",
        ".search-results a[href]",
        "a[href]",
    ]

    for selector in candidate_selectors:
        anchors = page.locator(selector)
        count = min(anchors.count(), 120)
        for idx in range(count):
            href = anchors.nth(idx).get_attribute("href")
            if not href:
                continue
            absolute = to_absolute(page.url, href)
            if is_company_like_url(absolute):
                return absolute

    return None


def looks_like_block_or_captcha(html: str) -> bool:
    text = html.lower()
    markers = [
        "captcha",
        "recaptcha",
        "hcaptcha",
        "cloudflare",
        "access denied",
        "forbidden",
        "unusual traffic",
        "prove you are human",
        "cf-chl",
        "/cdn-cgi/",
    ]
    return any(marker in text for marker in markers)


def save_debug_snapshot(page: Page, debug_dir: Path, prefix: str) -> None:
    ensure_dir(debug_dir)
    ts = time.strftime("%Y%m%d_%H%M%S")
    html_path = debug_dir / f"{prefix}_{ts}.html"
    png_path = debug_dir / f"{prefix}_{ts}.png"
    try:
        html_path.write_text(page.content(), encoding="utf-8")
        logging.warning("Saved debug HTML: %s", html_path)
    except Exception as exc:
        logging.warning("Failed to save debug HTML: %s", exc)
    try:
        page.screenshot(path=str(png_path), full_page=True)
        logging.warning("Saved debug screenshot: %s", png_path)
    except Exception as exc:
        logging.warning("Failed to save debug screenshot: %s", exc)


def safe_page_content(page: Page) -> str:
    try:
        return page.content()
    except Exception:
        return ""


def collect_links(
    start_inn: int,
    end_inn: int,
    output_csv: Path,
    config: ScraperConfig,
    max_errors: int,
    resume: bool,
    flush_every: int,
) -> None:
    if end_inn < start_inn:
        raise ValueError("end-inn must be >= start-inn")

    existing_links = read_existing_links(output_csv) if resume else set()
    if existing_links:
        logging.info("Loaded %s existing links from %s", len(existing_links), output_csv)

    captured_rows: List[Dict[str, str]] = []
    consecutive_errors = 0

    flush_every = max(1, flush_every)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=config.headless)
        context = browser.new_context(
            locale="ru-RU",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()
        page.set_default_timeout(config.timeout_ms)
        debug_dumped = False

        for inn in range(start_inn, end_inn + 1):
            try:
                search_url = urljoin(config.base_url, f"search/all/?q={inn}")
                page.goto(search_url, wait_until="domcontentloaded")
                if "/search/" not in page.url:
                    page.goto(config.base_url, wait_until="domcontentloaded")
                    search_input = find_search_input(page, config.search_input_selector)
                    if search_input is None:
                        raise RuntimeError("search input not found")
                    search_input.click()
                    search_input.fill(str(inn))
                    click_search(page, config.submit_selector)
                    try:
                        page.wait_for_url("**/search/**", timeout=min(config.timeout_ms, 12000))
                    except TimeoutError:
                        pass
                page.wait_for_load_state("domcontentloaded")
                sleep_random(config.min_delay, config.max_delay)

                link = extract_first_result_link(page, config.result_link_selector)
                if not link:
                    logging.debug("INN %s: no company result", inn)
                    consecutive_errors = 0
                    continue

                if link in existing_links:
                    logging.debug("INN %s: duplicate link %s", inn, link)
                    consecutive_errors = 0
                    continue

                row = {
                    "inn": str(inn),
                    "company_url": link,
                    "captured_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                }
                captured_rows.append(row)
                existing_links.add(link)
                consecutive_errors = 0
                logging.info("INN %s -> %s", inn, link)

                if len(captured_rows) >= flush_every:
                    append_links_rows(output_csv, captured_rows)
                    logging.info("Saved %s rows to %s", len(captured_rows), output_csv)
                    captured_rows.clear()

            except (TimeoutError, Error, RuntimeError) as exc:
                if captured_rows:
                    append_links_rows(output_csv, captured_rows)
                    logging.info("Saved %s buffered rows to %s after error", len(captured_rows), output_csv)
                    captured_rows.clear()
                consecutive_errors += 1
                logging.warning("INN %s failed: %s", inn, exc)
                if not debug_dumped:
                    save_debug_snapshot(page, config.debug_dir, f"collect_{inn}")
                    debug_dumped = True
                    content = safe_page_content(page)
                    if content and looks_like_block_or_captcha(content):
                        raise RuntimeError(
                            "Page looks blocked/captcha. Try --headed with slower delays and custom selectors. "
                            f"Check debug files in {config.debug_dir}"
                        ) from exc
                if consecutive_errors >= max_errors:
                    raise RuntimeError(
                        f"Stopped after {consecutive_errors} consecutive errors. "
                        f"Likely selectors/captcha/blocking changed. Check debug files in {config.debug_dir}"
                    ) from exc

        if captured_rows:
            append_links_rows(output_csv, captured_rows)
            logging.info("Saved final %s rows to %s", len(captured_rows), output_csv)

        context.close()
        browser.close()


def normalize_key(label: str) -> str:
    key = normalize_whitespace(label).lower()
    key = re.sub(r"[:\-]", "", key)
    key = re.sub(r"\s+", "_", key)
    key = re.sub(r"[^a-zа-я0-9_]+", "", key)
    return key.strip("_")


def extract_key_values_from_soup(soup: BeautifulSoup) -> Dict[str, str]:
    data: Dict[str, str] = {}

    # Pattern 1: table rows
    for tr in soup.select("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) >= 2:
            key = normalize_key(cells[0].get_text(" ", strip=True))
            value = normalize_whitespace(cells[1].get_text(" ", strip=True))
            if key and value and key not in data:
                data[key] = value

    # Pattern 2: definition lists
    for dl in soup.select("dl"):
        dts = dl.find_all("dt")
        for dt in dts:
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            key = normalize_key(dt.get_text(" ", strip=True))
            value = normalize_whitespace(dd.get_text(" ", strip=True))
            if key and value and key not in data:
                data[key] = value

    # Pattern 3: generic label/value blocks
    for parent in soup.select("div, li"):
        children = parent.find_all(recursive=False)
        if len(children) == 2:
            left = normalize_whitespace(children[0].get_text(" ", strip=True))
            right = normalize_whitespace(children[1].get_text(" ", strip=True))
            if not left or not right:
                continue
            if len(left) < 60 and len(right) < 300:
                key = normalize_key(left)
                if key and key not in data:
                    data[key] = right

    return data


def pick_first(text: str, patterns: Sequence[str]) -> str:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return normalize_whitespace(match.group(1))
    return ""


def clean_cell(value: str, default: str = "-") -> str:
    value = normalize_whitespace(value)
    if not value:
        return default
    if value in {"—", "–", "-", "yo'q", "Yo'q", "нет", "Нет"}:
        return default
    return value


def split_company_name_and_legal_form(name: str) -> tuple[str, str, str]:
    raw = clean_cell(name)
    if raw == "-":
        return "-", "-", "-"

    # Support common quote styles on orginfo pages.
    quote_pairs = [('\"', '\"'), ('«', '»'), ('“', '”')]
    for ql, qr in quote_pairs:
        if ql in raw and qr in raw:
            left = raw.find(ql)
            right = raw.find(qr, left + 1)
            if left != -1 and right != -1 and right > left + 1:
                core = clean_cell(raw[left + 1 : right])
                outside = (raw[:left] + " " + raw[right + 1 :]).strip(" ,.-")
                legal = clean_cell(outside, default="-")
                return core if core != "-" else raw, legal, raw

    return raw, "-", raw


def to_numeric_or_zero(value: str) -> str:
    value = normalize_whitespace(value)
    if not value:
        return "0"
    compact = value.replace("UZS", "").replace("uzs", "").replace(" ", "")
    compact = compact.split(",")[0].split(".")[0]
    nums = re.findall(r"\d+", compact)
    if not nums:
        return "0"
    return "".join(nums)


def first_existing(kv: Dict[str, str], keys: Sequence[str], default: str = "-") -> str:
    for key in keys:
        if key in kv:
            return clean_cell(kv[key], default=default)
    return default


def split_region_district(address: str) -> tuple[str, str]:
    if not address or address == "-":
        return "-", "-"
    parts = [normalize_whitespace(x) for x in address.split(",") if normalize_whitespace(x)]
    if not parts:
        return "-", "-"
    region = parts[0] if len(parts) >= 1 else "-"
    district = parts[1] if len(parts) >= 2 else "-"
    return region, district


def extract_company_fields(url: str, html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    page_text = normalize_whitespace(soup.get_text(" ", strip=True))
    kv = extract_key_values_from_soup(soup)

    title = normalize_whitespace(soup.title.get_text()) if soup.title else ""
    h1 = ""
    h1_node = soup.find("h1")
    if h1_node:
        h1 = normalize_whitespace(h1_node.get_text(" ", strip=True))

    emails = sorted(set(re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", page_text, flags=re.I)))
    phones = sorted(
        set(re.findall(r"\+?\d[\d\s\-()]{7,}\d", page_text)),
        key=len,
        reverse=True,
    )

    explicit_inn = ""
    for key, value in kv.items():
        if key in {"inn", "stir", "tin", "i_n_n"}:
            explicit_inn = value
            break

    if not explicit_inn:
        explicit_inn = pick_first(page_text, [r"(?:ИНН|STIR|TIN)\s*[:\-]?\s*(\d{6,14})"])

    company_name_full = first_existing(
        kv,
        ["официальное_название_организации", "organization_name", "tashkilot_nomi"],
        default=clean_cell(h1 or title),
    )
    company_name, legal_form, company_name_raw = split_company_name_and_legal_form(company_name_full)
    short_name = first_existing(kv, ["краткое_название_организации", "qisqa_nomi"])
    registration_date = first_existing(kv, ["royxatdan_otgan_sana", "дата_регистрации"])
    activity_status = first_existing(kv, ["faollik_holati", "статус"])
    registration_authority = first_existing(kv, ["royxatdan_otkazuvchi_organ", "регистрирующий_орган"])
    thsht = first_existing(kv, ["thsht"])
    dbibt = first_existing(kv, ["dbibt"])
    ifut = first_existing(kv, ["ifut"])
    charter_capital_raw = first_existing(kv, ["ustav_fondi", "уставной_фонд"], default="")
    charter_capital = to_numeric_or_zero(charter_capital_raw)
    email = first_existing(kv, ["elektron_pochta", "email"], default=emails[0] if emails else "-")
    phone = first_existing(kv, ["telefon_raqami", "phone"], default=phones[0] if phones else "-")
    address = first_existing(kv, ["manzili", "address", "адрес"])
    region, district = split_region_district(address)
    category = first_existing(kv, ["toifa", "категория"])
    tax_committee = first_existing(kv, ["soliq_qomitasi", "налоговый_комитет"])
    large_taxpayer = first_existing(kv, ["yirik_soliq_tolovchi", "крупный_налогоплательщик"])
    director = first_existing(kv, ["rahbar", "director", "руководитель"])
    founders = first_existing(kv, ["tasischilar", "учредители"])

    inn = clean_cell(explicit_inn)
    if inn == "-":
        inn = first_existing(kv, ["stir", "inn", "tin"])

    return {
        "source_url": clean_cell(url),
        "company_name": company_name,
        "company_name_raw": company_name_raw,
        "legal_form": legal_form,
        "short_name": short_name,
        "inn": inn,
        "registration_date": registration_date,
        "activity_status": activity_status,
        "registration_authority": registration_authority,
        "thsht": thsht,
        "dbibt": dbibt,
        "ifut": ifut,
        "charter_capital_uzs": charter_capital,
        "email": clean_cell(email),
        "phone": clean_cell(phone),
        "address": address,
        "region": region,
        "district": district,
        "category": category,
        "tax_committee": tax_committee,
        "large_taxpayer": large_taxpayer,
        "director": director,
        "founders": founders,
        "employees_count": "0",
        "branch_count": "0",
    }


def is_valid_company_row(row: Dict[str, str]) -> bool:
    name = normalize_whitespace(row.get("company_name", "")).lower()
    inn = normalize_whitespace(row.get("inn", ""))
    blocked_names = {
        "500",
        "404",
        "403",
        "internal server error",
        "bad gateway",
        "gateway timeout",
        "error",
    }
    if not name or name in blocked_names:
        return False
    if inn in {"", "-", "0"}:
        return False
    return True


def iter_links_from_csv(path: Path) -> Iterable[str]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = (row.get("company_url") or "").strip()
            if url:
                yield url


def parse_companies(
    links_csv: Path,
    output_csv: Path,
    config: ScraperConfig,
    max_companies: Optional[int],
) -> None:
    if not links_csv.exists():
        raise FileNotFoundError(f"Links CSV not found: {links_csv}")

    rows: List[Dict[str, str]] = []
    links = list(dict.fromkeys(iter_links_from_csv(links_csv)))
    if max_companies:
        links = links[:max_companies]

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=config.headless)
        context = browser.new_context(
            locale="ru-RU",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()
        page.set_default_timeout(config.timeout_ms)

        for idx, url in enumerate(links, start=1):
            try:
                page.goto(url, wait_until="domcontentloaded")
                html = page.content()
                row = extract_company_fields(url, html)
                if not is_valid_company_row(row):
                    logging.warning("%s/%s skipped invalid card %s", idx, len(links), url)
                    continue
                rows.append(row)
                logging.info("%s/%s parsed %s", idx, len(links), url)
                sleep_random(config.min_delay, config.max_delay)
            except (TimeoutError, Error) as exc:
                logging.warning("Failed to parse %s: %s", url, exc)

        context.close()
        browser.close()

    write_company_rows(output_csv, rows)
    logging.info("Saved %s companies to %s", len(rows), output_csv)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Orginfo.uz scraper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--headed", action="store_true", help="Run browser in headed mode")
    common.add_argument("--timeout-ms", type=int, default=20000, help="Playwright timeout in ms")
    common.add_argument("--min-delay", type=float, default=0.4, help="Min delay between requests")
    common.add_argument("--max-delay", type=float, default=1.2, help="Max delay between requests")
    common.add_argument("--verbose", action="store_true", help="Enable debug logs")
    common.add_argument("--base-url", default=BASE_URL, help="Search page URL")
    common.add_argument("--search-input-selector", default="", help="Manual CSS selector for search input")
    common.add_argument("--submit-selector", default="", help="Manual CSS selector for search submit button")
    common.add_argument("--result-link-selector", default="", help="Manual CSS selector for result links")
    common.add_argument("--debug-dir", type=Path, default=Path("data/debug"), help="Directory for debug dumps")

    collect = subparsers.add_parser("collect-links", parents=[common])
    collect.add_argument("--start-inn", type=int, required=True)
    collect.add_argument("--end-inn", type=int, required=True)
    collect.add_argument("--output-csv", type=Path, default=DEFAULT_LINKS_CSV)
    collect.add_argument("--max-errors", type=int, default=15)
    collect.add_argument("--flush-every", type=int, default=1, help="Write buffered links every N found rows")
    collect.add_argument("--no-resume", action="store_true", help="Ignore existing links csv")

    parse = subparsers.add_parser("parse-companies", parents=[common])
    parse.add_argument("--links-csv", type=Path, default=DEFAULT_LINKS_CSV)
    parse.add_argument("--output-csv", type=Path, default=DEFAULT_COMPANIES_CSV)
    parse.add_argument("--max-companies", type=int, default=None)

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    setup_logging(args.verbose)

    config = ScraperConfig(
        headless=not args.headed,
        timeout_ms=args.timeout_ms,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        base_url=args.base_url,
        search_input_selector=args.search_input_selector,
        submit_selector=args.submit_selector,
        result_link_selector=args.result_link_selector,
        debug_dir=args.debug_dir,
    )

    try:
        if args.command == "collect-links":
            collect_links(
                start_inn=args.start_inn,
                end_inn=args.end_inn,
                output_csv=args.output_csv,
                config=config,
                max_errors=args.max_errors,
                resume=not args.no_resume,
                flush_every=args.flush_every,
            )
        elif args.command == "parse-companies":
            parse_companies(
                links_csv=args.links_csv,
                output_csv=args.output_csv,
                config=config,
                max_companies=args.max_companies,
            )
        else:
            raise ValueError(f"Unknown command: {args.command}")
    except Exception as exc:
        logging.error("Scraper failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
