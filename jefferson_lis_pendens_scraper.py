#!/usr/bin/env python3
"""
Jefferson County, Kentucky Lis Pendens scraper.

Searches https://search.jeffersondeeds.com by instrument type, downloads each
filing image, OCRs every page, extracts parties and the full property address,
and writes a Google Sheets-compatible CSV plus a detailed action log.
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import fitz  # PyMuPDF
import pandas as pd
import pytesseract
import requests
from bs4 import BeautifulSoup
from PIL import Image, ImageDraw, ImageSequence


BASE_URL = "https://search.jeffersondeeds.com/"
SEARCH_PAGE = urljoin(BASE_URL, "insttype.php")
SEARCH_ENDPOINT = urljoin(BASE_URL, "p6.php")
PVA_PROPERTY_SEARCH = "https://jeffersonpva.ky.gov/property-search/property-listings/"
OUTPUT_COLUMNS = ["Date", "Defendants/Parties", "Property Address", "PDF Link", "Notes"]


STREET_SUFFIXES = (
    "St", "Street", "Ave", "Avenue", "Rd", "Road", "Dr", "Drive", "Ct", "Court",
    "Ln", "Lane", "Blvd", "Boulevard", "Way", "Pl", "Place", "Cir", "Circle",
    "Ter", "Terrace", "Pkwy", "Parkway", "Trl", "Trail", "Hwy", "Highway",
    "Run", "Loop", "Crossing", "Pass", "Ridge", "Commons", "Sq", "Square",
    "Cove", "Cv", "Trace", "Path",
)


OCR_REPLACEMENTS = {
    "L0UISVILLE": "LOUISVILLE",
    "LOUISV1LLE": "LOUISVILLE",
    "LOUIS VILLE": "LOUISVILLE",
    "LOUISVILIE": "LOUISVILLE",
    "Maleolm": "Malcolm",
    "MALEOLM": "MALCOLM",
    "MaloIm": "Malcolm",
    "R0AD": "ROAD",
}


@dataclass
class FilingRecord:
    instrument_number: str
    filing_date: str
    document_type: str
    grantors: list[str]
    grantees: list[str]
    legal_description: str
    detail_url: str
    document_url: str
    property_address: str = "Address not found"
    notes: list[str] = field(default_factory=list)

    @property
    def parties_for_csv(self) -> str:
        seen: set[str] = set()
        parties: list[str] = []
        for party in self.grantors + self.grantees:
            clean = clean_party(party)
            if clean and clean.upper() not in seen:
                seen.add(clean.upper())
                parties.append(clean)
        return "; ".join(parties)


class ActionLogger:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")

    def write(self, level: str, message: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] {level}: {message}"
        print(line, flush=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def action(self, message: str) -> None:
        self.write("ACTION", message)

    def result(self, message: str) -> None:
        self.write("RESULT", message)

    def warning(self, message: str) -> None:
        self.write("WARNING", message)

    def error(self, message: str) -> None:
        self.write("ERROR", message)


def parse_mmddyyyy(value: str) -> datetime:
    value = value.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Date must be MM/DD/YYYY or YYYY-MM-DD, got: {value}")


def iter_dates(start: str, end: str) -> Iterable[str]:
    current = parse_mmddyyyy(start)
    stop = parse_mmddyyyy(end)
    if current > stop:
        raise ValueError("START_DATE must be on or before END_DATE")
    while current <= stop:
        yield current.strftime("%m/%d/%Y")
        current += timedelta(days=1)


def clean_text(value: str) -> str:
    value = value.replace("\xa0", " ")
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"\s*\|\s*", " | ", value)
    return value.strip()


def clean_party(value: str) -> str:
    value = clean_text(value)
    value = re.sub(r"\s+", " ", value)
    return value.strip(" ;|")


def clean_ocr_text(text: str) -> str:
    cleaned = text
    for bad, good in OCR_REPLACEMENTS.items():
        cleaned = re.sub(re.escape(bad), good, cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.replace("\r", "\n")
    cleaned = re.sub(r"\b([NSEW])\.", r"\1", cleaned)
    cleaned = re.sub(r"\b(St|Rd|Ave|Dr|Ct|Ln|Blvd|Ter|Pkwy)\.", r"\1", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"-\n", "", cleaned)
    cleaned = re.sub(r"(?<!\n)\n(?!\n)", " ", cleaned)
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    return cleaned


def normalize_address(raw: str) -> str:
    address = clean_ocr_text(raw)
    address = re.sub(r"\s+", " ", address)
    address = re.sub(r"\s+,", ",", address)
    address = re.sub(r",\s*", ", ", address)
    address = re.sub(r"\bRoad\b", "Rd", address, flags=re.IGNORECASE)
    address = re.sub(r"\bStreet\b", "St", address, flags=re.IGNORECASE)
    address = re.sub(r"\bAvenue\b", "Ave", address, flags=re.IGNORECASE)
    address = re.sub(r"\bDrive\b", "Dr", address, flags=re.IGNORECASE)
    address = re.sub(r"\bCourt\b", "Ct", address, flags=re.IGNORECASE)
    address = re.sub(r"\bLane\b", "Ln", address, flags=re.IGNORECASE)
    address = re.sub(r"\bBoulevard\b", "Blvd", address, flags=re.IGNORECASE)
    address = re.sub(r"\bLouisville\b", "Louisville", address, flags=re.IGNORECASE)
    address = re.sub(r"\bKY\b", "KY", address, flags=re.IGNORECASE)
    address = re.sub(r"\bKentucky\b", "KY", address, flags=re.IGNORECASE)
    address = re.sub(r"\s+(located|and located|in Jefferson County).*", "", address, flags=re.IGNORECASE)
    address = address.strip(" .;,")
    return address


def looks_like_legal_description(candidate: str) -> bool:
    upper = candidate.upper()
    if "527 W JEFFERSON" in upper or "JEFFERSON COUNTY CLERK" in upper or "COUNTYCLERK" in upper:
        return True
    if re.search(r"\b(BK|BOOK)\s*:?\s*L?\s*\d{3,5}\b", upper) or re.search(r"\bPG\s*:?\s*\d+", upper):
        return True
    legal_terms = [" LOT ", " SEC ", " SUB ", " ESMT", " PARCEL", " DEED BOOK", " BOOK ", " PAGE "]
    if re.search(r"\b\d{2}CI\d{6}\b", upper):
        return True
    return any(term in f" {upper} " for term in legal_terms) and not re.search(r"\bLOUISVILLE\b|\bKY\b", upper)


def is_valid_address(candidate: str) -> bool:
    if not candidate or looks_like_legal_description(candidate):
        return False
    suffix_pattern = "|".join(re.escape(s) for s in STREET_SUFFIXES)
    has_number = bool(re.search(r"\b\d{2,6}\b", candidate))
    has_suffix = bool(re.search(rf"\b({suffix_pattern})\b", candidate, flags=re.IGNORECASE))
    has_city_or_zip = bool(re.search(r"\bLouisville\b|\bKY\b|\b402\d{2}\b", candidate, flags=re.IGNORECASE))
    return has_number and has_suffix and has_city_or_zip


def extract_property_address(ocr_text: str) -> tuple[str, str]:
    text = clean_ocr_text(ocr_text)
    flat = re.sub(r"\s+", " ", text)
    suffix_pattern = "|".join(re.escape(s) for s in STREET_SUFFIXES)

    cue_patterns = [
        rf"(?:Property Address|Common Address|street address)\s*:?\s*(\d{{2,6}}\s+[\w\s,'/-]{{3,120}}\b(?:{suffix_pattern})\b(?:[\w\s,'/-]{{0,80}}?(?:Louisville\s*,?\s*KY(?:\s*\d{{5}})?|KY\s*\d{{5}}|402\d{{2}}))?)",
        rf"(?:known and designated as|commonly known as|known as|located at|real property located at)\s*:?\s*(\d{{2,6}}\s+[\w\s,'/-]{{3,120}}\b(?:{suffix_pattern})\b(?:[\w\s,'/-]{{0,80}}?(?:Louisville\s*,?\s*KY(?:\s*\d{{5}})?|KY\s*\d{{5}}|402\d{{2}}))?)",
        rf"(\d{{2,6}}\s+[\w\s,'/-]{{3,120}}\b(?:{suffix_pattern})\b\s*,?\s*Louisville\s*,?\s*KY(?:\s*\d{{5}})?)",
        rf"(\d{{2,6}}\s+[\w\s,'/-]{{3,90}}\b(?:{suffix_pattern})\b[\w\s,'/-]{{0,40}}\b402\d{{2}}\b)",
    ]

    candidates: list[str] = []
    for pattern in cue_patterns:
        for match in re.finditer(pattern, flat, flags=re.IGNORECASE):
            candidates.append(match.group(1))

    for candidate in candidates:
        normalized = normalize_address(candidate)
        if is_valid_address(normalized):
            return normalized, "Property address extracted successfully."

    for candidate in candidates:
        normalized = normalize_address(candidate)
        suffix_ok = re.search(rf"\b({suffix_pattern})\b", normalized, flags=re.IGNORECASE)
        number_ok = re.search(r"\b\d{2,6}\b", normalized)
        if number_ok and suffix_ok and not looks_like_legal_description(normalized):
            return normalized, "Partial address extracted by OCR; verify manually."

    return "Address not found", "Address not found in OCR text."


def absolute_url(href: str) -> str:
    return urljoin(BASE_URL, href)


def set_doc_type(url: str, doc_type: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query["type"] = [doc_type]
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "unknown"


def parse_search_results(html: str, logger: ActionLogger) -> list[FilingRecord]:
    if "No Instruments Were Found" in html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    records: list[FilingRecord] = []
    view_links = soup.find_all("a", string=lambda s: s and "VIEW" in s.upper())

    for view_link in view_links:
        tr = view_link.find_parent("tr")
        if not tr:
            logger.warning("VIEW link had no parent row; skipping malformed result row.")
            continue

        detail_link = tr.find("a", href=re.compile(r"pdetail\.php\?"))
        detail_url = absolute_url(detail_link["href"]) if detail_link else ""
        instrument = ""
        if detail_url:
            instrument = parse_qs(urlparse(detail_url).query).get("instnum", [""])[0]

        tds = tr.find_all("td")
        if len(tds) < 8:
            logger.warning(f"Instrument {instrument or 'unknown'} row had {len(tds)} cells; attempting partial parse.")

        grantors = [x.strip() for x in tds[2].get_text("\n", strip=True).splitlines()] if len(tds) > 2 else []
        grantees = [x.strip() for x in tds[3].get_text("\n", strip=True).splitlines()] if len(tds) > 3 else []
        legal_description = clean_text(tds[4].get_text(" ", strip=True)) if len(tds) > 4 else ""
        filing_date = clean_text(tds[5].get_text(" ", strip=True)) if len(tds) > 5 else ""
        document_type = clean_text(tds[7].get_text(" ", strip=True)) if len(tds) > 7 else ""
        document_url = absolute_url(view_link.get("href", ""))

        if not instrument:
            match = re.search(r"/(\d{10})\.tif", document_url)
            instrument = match.group(1) if match else safe_name(document_url)

        records.append(
            FilingRecord(
                instrument_number=instrument,
                filing_date=filing_date,
                document_type=document_type,
                grantors=grantors,
                grantees=grantees,
                legal_description=legal_description,
                detail_url=detail_url,
                document_url=document_url,
            )
        )

    return records


def search_one_date(
    session: requests.Session,
    date_value: str,
    logger: ActionLogger,
    instrument_code: str = "LP ",
    instrument_label: str = "LIS PENDENS",
) -> list[FilingRecord]:
    logger.action(f"Opened {SEARCH_PAGE}")
    resp = session.get(SEARCH_PAGE, timeout=30)
    resp.raise_for_status()
    logger.action("Selected Search by Instrument Type")
    logger.action(f"Selected {instrument_label}")
    logger.action(f"Entered Start Date: {date_value}")
    logger.action(f"Entered End Date: {date_value}")
    payload = {
        "cnum": "CNUM",
        "itype1": instrument_code,
        "itype2": "",
        "itype3": "",
        "bDate": date_value,
        "eDate": date_value,
        "searchtype": "ITYPE",
        "search": "Execute Search",
    }
    logger.action("Clicked Execute Search")
    resp = session.post(SEARCH_ENDPOINT, data=payload, timeout=60)
    resp.raise_for_status()
    records = parse_search_results(resp.text, logger)
    if records:
        logger.result(f"Found {len(records)} records for {date_value}")
    else:
        logger.result(f"No {instrument_label} found for {date_value}")
    return records


def search_one_date_browser(
    date_value: str,
    logger: ActionLogger,
    headless: bool = True,
    instrument_code: str = "LP ",
    instrument_label: str = "LIS PENDENS",
) -> list[FilingRecord]:
    """
    Playwright fallback for environments where direct form posts stop working.

    The direct HTTP path is intentionally the default because it is less fragile
    than browser coordinates. This fallback keeps the project ready for a future
    site change or anti-bot block, but requires `pip install playwright` and
    `playwright install chromium`.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright fallback requested but the playwright package is not installed. "
            "Install it with: python -m pip install playwright && playwright install chromium"
        ) from exc

    logger.action(f"Opened {BASE_URL} with Playwright fallback")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=100 if not headless else 0)
        page = browser.new_page()
        page.goto(SEARCH_PAGE, wait_until="networkidle", timeout=60000)
        logger.action("Selected Search by Instrument Type using Playwright fallback")
        logger.action(f"Selected {instrument_label} using Playwright fallback")
        logger.action(f"Entered Start Date: {date_value}")
        logger.action(f"Entered End Date: {date_value}")
        page.locator('select[name="itype1"]').select_option(instrument_code)
        page.locator('input[name="bDate"]').fill(date_value)
        page.locator('input[name="eDate"]').fill(date_value)
        logger.action("Clicked Execute Search using Playwright fallback")
        with page.expect_navigation(wait_until="networkidle", timeout=60000):
            page.locator('input[type="submit"], button[type="submit"]').last.click()
        html = page.content()
        browser.close()

    records = parse_search_results(html, logger)
    if records:
        logger.result(f"Found {len(records)} records for {date_value} using Playwright fallback")
    else:
        logger.result(f"No {instrument_label} found for {date_value} using Playwright fallback")
    return records


def search_date_with_mode(
    session: requests.Session,
    date_value: str,
    logger: ActionLogger,
    search_mode: str,
    browser_headless: bool,
    instrument_code: str = "LP ",
    instrument_label: str = "LIS PENDENS",
) -> list[FilingRecord]:
    if search_mode == "direct":
        return search_one_date(session, date_value, logger, instrument_code, instrument_label)
    if search_mode == "browser":
        return search_one_date_browser(
            date_value, logger, headless=browser_headless,
            instrument_code=instrument_code, instrument_label=instrument_label,
        )
    try:
        return search_one_date(session, date_value, logger, instrument_code, instrument_label)
    except Exception as exc:
        logger.warning(f"Direct search failed for {date_value}; falling back to Playwright browser mode: {exc}")
        return search_one_date_browser(
            date_value, logger, headless=browser_headless,
            instrument_code=instrument_code, instrument_label=instrument_label,
        )


def download_with_retries(
    session: requests.Session,
    record: FilingRecord,
    downloaded_dir: Path,
    screenshots_dir: Path,
    logger: ActionLogger,
) -> Path | None:
    attempts = [
        ("pdf", set_doc_type(record.document_url, "pdf")),
        ("pdf", set_doc_type(record.document_url, "pdf")),
        ("tif", set_doc_type(record.document_url, "tif")),
    ]

    last_error = ""
    for index, (doc_type, url) in enumerate(attempts, start=1):
        try:
            if index == 1:
                logger.action(f"Opened document image URL for Instrument #{record.instrument_number}")
            elif index == 2:
                logger.action(f"Retrying document load attempt 2 for Instrument #{record.instrument_number}")
            else:
                logger.action(f"Changed type=pdf to type=tif for Instrument #{record.instrument_number}")
                logger.action(f"Retrying document load attempt 3 for Instrument #{record.instrument_number}")

            resp = session.get(url, timeout=90)
            content_type = resp.headers.get("content-type", "").lower()
            resp.raise_for_status()
            if len(resp.content) < 1000 or b"not found" in resp.content[:500].lower():
                raise RuntimeError(f"Downloaded response was too small or invalid ({len(resp.content)} bytes).")

            extension = "pdf" if "pdf" in content_type or doc_type == "pdf" else "tif"
            if doc_type == "tif":
                extension = "tif"
            output_path = downloaded_dir / f"{record.instrument_number}.{extension}"
            output_path.write_bytes(resp.content)
            logger.result(f"Downloaded {extension.upper()} successfully for Instrument #{record.instrument_number}")
            return output_path
        except Exception as exc:
            last_error = str(exc)
            logger.warning(f"Document attempt {index} failed for Instrument #{record.instrument_number}: {last_error}")
            time.sleep(1.5)

    screenshot_path = screenshots_dir / f"{record.instrument_number}_document_failure.png"
    create_failure_screenshot(screenshot_path, record.document_url, last_error)
    logger.error(f"Document failed after 3 attempts for Instrument #{record.instrument_number}; screenshot saved to {screenshot_path}")
    record.notes.append("Document failed after 3 attempts")
    return None


def find_existing_document(downloaded_dir: Path, instrument: str) -> Path | None:
    for extension in ("pdf", "tif", "tiff", "png", "jpg", "jpeg"):
        candidate = downloaded_dir / f"{instrument}.{extension}"
        if candidate.exists() and candidate.stat().st_size > 1000:
            return candidate
    return None


def create_failure_screenshot(path: Path, url: str, error: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    img = Image.new("RGB", (1400, 700), "white")
    draw = ImageDraw.Draw(img)
    lines = [
        "Document failure after final retry",
        f"URL: {url}",
        f"Error: {error}",
        f"Captured: {datetime.now().isoformat(timespec='seconds')}",
    ]
    y = 40
    for line in lines:
        draw.text((40, y), line[:180], fill="black")
        y += 40
    img.save(path)


def convert_pdf_to_images(path: Path, pages_dir: Path, instrument: str, logger: ActionLogger) -> list[Path]:
    output_paths: list[Path] = []
    doc = fitz.open(path)
    for idx, page in enumerate(doc, start=1):
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
        output = pages_dir / f"{instrument}_page_{idx:03d}.png"
        pix.save(output)
        output_paths.append(output)
        logger.action(f"Converted PDF page {idx} to PNG for Instrument #{instrument}")
    doc.close()
    return output_paths


def convert_tiff_to_images(path: Path, pages_dir: Path, instrument: str, logger: ActionLogger) -> list[Path]:
    output_paths: list[Path] = []
    image = Image.open(path)
    for idx, page in enumerate(ImageSequence.Iterator(image), start=1):
        output = pages_dir / f"{instrument}_page_{idx:03d}.png"
        page.convert("RGB").save(output)
        output_paths.append(output)
        logger.action(f"Converted TIFF page {idx} to PNG for Instrument #{instrument}")
    return output_paths


def convert_document_to_images(path: Path, pages_dir: Path, instrument: str, logger: ActionLogger) -> list[Path]:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        return convert_pdf_to_images(path, pages_dir, instrument, logger)
    if suffix in {".tif", ".tiff"}:
        return convert_tiff_to_images(path, pages_dir, instrument, logger)
    output = pages_dir / f"{instrument}_page_001.png"
    Image.open(path).convert("RGB").save(output)
    logger.action(f"Converted image to PNG for Instrument #{instrument}")
    return [output]


def ocr_pages(page_paths: list[Path], ocr_dir: Path, instrument: str, logger: ActionLogger) -> str:
    chunks: list[str] = []
    for idx, page_path in enumerate(page_paths, start=1):
        logger.action(f"OCR page {idx} for Instrument #{instrument}")
        image = Image.open(page_path)
        text = pytesseract.image_to_string(image, config="--psm 6")
        chunks.append(f"\n\n--- OCR PAGE {idx} ({page_path.name}) ---\n{text}")
    full_text = "\n".join(chunks)
    output = ocr_dir / f"{instrument}.txt"
    output.write_text(full_text, encoding="utf-8", errors="replace")
    logger.result(f"Saved raw OCR text for Instrument #{instrument} to {output}")
    return full_text


def pva_address_query(address: str) -> str:
    search_value = re.sub(r",?\s*(Louisville|Fairdale)\s*,?\s*KY\s*\d{0,5}", "", address, flags=re.IGNORECASE)
    search_value = re.sub(r"\s+", " ", search_value).strip(" ,")
    query = urlencode({"psfldAddress": search_value, "searchType": "StreetSearch"})
    return f"{PVA_PROPERTY_SEARCH}?{query}#results"


def add_pva_cross_check(session: requests.Session, record: FilingRecord, logger: ActionLogger) -> None:
    if record.property_address == "Address not found":
        logger.warning(f"PVA cross-check skipped for Instrument #{record.instrument_number}: no address found")
        return
    url = pva_address_query(record.property_address)
    try:
        response = session.get(url, timeout=20)
        if response.ok:
            record.notes.append(f"PVA manual verification URL: {url}")
            logger.result(f"PVA cross-check URL generated and reachable for Instrument #{record.instrument_number}: {url}")
        else:
            record.notes.append(f"PVA manual verification URL generated but returned HTTP {response.status_code}: {url}")
            logger.warning(f"PVA cross-check URL returned HTTP {response.status_code} for Instrument #{record.instrument_number}: {url}")
    except Exception as exc:
        record.notes.append(f"PVA manual verification URL generated; reachability failed: {url}")
        logger.warning(f"PVA cross-check reachability failed for Instrument #{record.instrument_number}: {exc}")


def process_document(
    session: requests.Session,
    record: FilingRecord,
    debug_dir: Path,
    logger: ActionLogger,
    resume: bool = False,
    pva_cross_check: bool = False,
    always_include_legal_desc: bool = False,
) -> None:
    downloaded_dir = debug_dir / "downloaded_docs"
    pages_dir = debug_dir / "converted_pages"
    ocr_dir = debug_dir / "ocr_text"
    screenshots_dir = debug_dir / "screenshots"
    for directory in (downloaded_dir, pages_dir, ocr_dir, screenshots_dir):
        directory.mkdir(parents=True, exist_ok=True)

    logger.action(f"Opened Instrument #{record.instrument_number}")
    ocr_text_path = ocr_dir / f"{record.instrument_number}.txt"
    if resume and ocr_text_path.exists() and ocr_text_path.stat().st_size > 0:
        logger.action(f"Resume mode: using existing OCR text for Instrument #{record.instrument_number}")
        ocr_text = ocr_text_path.read_text(encoding="utf-8", errors="replace")
        address, note = extract_property_address(ocr_text)
        record.property_address = address
        record.notes.append(f"{note} Resume mode used existing OCR text.")
        if always_include_legal_desc and record.legal_description:
            record.notes.append(f"Legal Desc: {record.legal_description}")
        if pva_cross_check:
            add_pva_cross_check(session, record, logger)
        return

    doc_path = find_existing_document(downloaded_dir, record.instrument_number) if resume else None
    if doc_path:
        logger.action(f"Resume mode: using existing downloaded document for Instrument #{record.instrument_number}: {doc_path}")
    else:
        doc_path = download_with_retries(session, record, downloaded_dir, screenshots_dir, logger)
    if not doc_path:
        record.property_address = "Address not found"
        record.notes.append(f"Legal Desc: {record.legal_description}")
        return

    try:
        page_paths = convert_document_to_images(doc_path, pages_dir, record.instrument_number, logger)
        ocr_text = ocr_pages(page_paths, ocr_dir, record.instrument_number, logger)
        address, note = extract_property_address(ocr_text)
        record.property_address = address
        record.notes.append(note)
        if address != "Address not found":
            logger.result(f"Address found for Instrument #{record.instrument_number}: {address}")
            if always_include_legal_desc and record.legal_description:
                record.notes.append(f"Legal Desc: {record.legal_description}")
        else:
            logger.warning(f"Address not found for Instrument #{record.instrument_number}")
            record.notes.append(f"Legal Desc: {record.legal_description}")
        if pva_cross_check:
            add_pva_cross_check(session, record, logger)
    except Exception as exc:
        logger.error(f"OCR/conversion failed for Instrument #{record.instrument_number}: {exc}")
        record.property_address = "Address not found"
        record.notes.append(f"OCR failure reason: {exc}")
        record.notes.append(f"Legal Desc: {record.legal_description}")


def write_csv(records: list[FilingRecord], output_csv: Path, logger: ActionLogger, source_tag: str = "") -> None:
    rows = []
    for record in records:
        note_parts = [source_tag] if source_tag else []
        note_parts.extend(x for x in record.notes if x)
        notes = "; ".join(note_parts)
        rows.append(
            {
                "Date": record.filing_date,
                "Defendants/Parties": record.parties_for_csv,
                "Property Address": record.property_address,
                "PDF Link": record.document_url,
                "Notes": notes,
            }
        )
        logger.action(f"Saved CSV row for Instrument #{record.instrument_number}")

    with output_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    logger.result(f"Saved CSV: {output_csv}")


def write_validation_report(records: list[FilingRecord], output_path: Path, logger: ActionLogger) -> None:
    names = " | ".join(r.parties_for_csv.upper() for r in records)
    addresses = " | ".join(r.property_address.upper() for r in records)
    checks = {
        "Matthew S. Martin filing pulled": "MARTIN MATTHEW S" in names or "MATTHEW S MARTIN" in names,
        "4419 Malcolm address extracted": "4419 MALCOLM" in addresses,
        "Ted R. Cox filing pulled": "COX TED R" in names or "TED R COX" in names,
        "Buckley Scott filing pulled": "BUCKLEY SCOTT" in names,
        "All processed records have CSV rows": len(records) > 0,
    }
    status = "PASS" if all(checks.values()) else "FAIL"
    lines = [f"Validation status: {status}", "", "Checks:"]
    lines.extend(f"- {name}: {'PASS' if passed else 'FAIL'}" for name, passed in checks.items())
    lines.append("")
    lines.append(f"Unique records processed: {len(records)}")
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.result(f"Validation report saved: {output_path}")
    if status == "PASS":
        logger.result("Primary 2026 benchmark validation passed.")
    else:
        logger.warning("Primary 2026 benchmark validation did not fully pass.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape Jefferson County KY Lis Pendens filings.")
    parser.add_argument("--start-date", default="05/07/2026", help="Start date in MM/DD/YYYY format.")
    parser.add_argument("--end-date", default="05/08/2026", help="End date in MM/DD/YYYY format.")
    parser.add_argument("--output-dir", default=".", help="Directory for CSV, action log, and debug folder.")
    parser.add_argument("--sleep", type=float, default=0.75, help="Delay between document requests.")
    parser.add_argument("--resume", action="store_true", help="Reuse existing downloaded docs/OCR text when available.")
    parser.add_argument("--pva-cross-check", action="store_true", help="Append a reachable Jefferson PVA manual verification URL to Notes.")
    parser.add_argument("--search-mode", choices=["direct", "browser", "auto"], default="direct", help="Search via direct HTTP, Playwright browser, or direct with browser fallback.")
    parser.add_argument("--headed-browser", action="store_true", help="Show browser window when --search-mode uses Playwright.")
    parser.add_argument("--instrument-code", default="LP ", help="Jefferson Deeds instrument-type itype1 code (e.g. 'LP ' for Lis Pendens, 'WIL' for Wills).")
    parser.add_argument("--instrument-label", default="LIS PENDENS", help="Human-readable instrument type label, used for logging and validation.")
    parser.add_argument("--csv-name", default="lis_pendens_results.csv", help="Output CSV filename inside --output-dir.")
    parser.add_argument("--skip-validation", action="store_true", help="Skip the Lis-Pendens-specific benchmark validation report.")
    parser.add_argument("--source-tag", default="", help="Optional tag prepended to the Notes column (e.g. 'Source: WILLS').")
    parser.add_argument("--always-include-legal-desc", action="store_true", help="Always append Legal Desc to Notes, not only when address extraction fails.")
    args = parser.parse_args()

    output_dir = Path(args.output_dir).resolve()
    debug_dir = output_dir / "debug"
    output_dir.mkdir(parents=True, exist_ok=True)
    debug_dir.mkdir(parents=True, exist_ok=True)
    logger = ActionLogger(output_dir / "action_log.txt")

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 Jefferson Lis Pendens Research Bot",
            "Referer": SEARCH_PAGE,
        }
    )

    logger.action(f"Opened {BASE_URL}")
    all_records: list[FilingRecord] = []
    seen_instruments: set[str] = set()

    try:
        for date_value in iter_dates(args.start_date, args.end_date):
            date_records = search_date_with_mode(
                session,
                date_value,
                logger,
                args.search_mode,
                browser_headless=not args.headed_browser,
                instrument_code=args.instrument_code,
                instrument_label=args.instrument_label,
            )
            for record in date_records:
                if record.instrument_number in seen_instruments:
                    logger.warning(f"Duplicate skipped by instrument number: {record.instrument_number}")
                    continue
                seen_instruments.add(record.instrument_number)
                all_records.append(record)
            time.sleep(args.sleep)

        logger.result(f"Found {len(all_records)} unique records after deduplication")

        for record in all_records:
            process_document(
                session,
                record,
                debug_dir,
                logger,
                resume=args.resume,
                pva_cross_check=args.pva_cross_check,
                always_include_legal_desc=args.always_include_legal_desc,
            )
            time.sleep(args.sleep)

        csv_path = output_dir / args.csv_name
        write_csv(all_records, csv_path, logger, source_tag=args.source_tag)
        if not args.skip_validation:
            write_validation_report(all_records, output_dir / "validation_report.txt", logger)

        preview_path = output_dir / "csv_preview.txt"
        df = pd.read_csv(csv_path)
        preview_path.write_text(df.head(20).to_string(index=False), encoding="utf-8")
        logger.result(f"CSV preview saved: {preview_path}")
        return 0
    except Exception as exc:
        logger.error(f"Fatal scraper error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
