"""
Business website scraper for ad generation pipeline.
Reads a CSV of businesses, scrapes each website, outputs enriched JSONL profiles.

Expected CSV columns (any one name per field is accepted):
  - name:            "name" or "Business Name"
  - website:         "website" or "Website"
  - location:        "location" or "Location (address)"
  - business_type:   "business_type" or "What they do (primary category)"

Default input: alamo, ca_businesses.csv
Output: profiles.jsonl  (one JSON object per business)
Failures: failed.jsonl
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MAX_CONCURRENT = 20
REQUEST_TIMEOUT = 12
PLAYWRIGHT_TIMEOUT = 18
JS_FALLBACK_THRESHOLD = 200
INPUT_FILE = "alamo, ca_businesses.csv"
OUTPUT_FILE = "profiles.jsonl"
FAILED_FILE = "failed.jsonl"

ABOUT_PATHS = ["/about", "/about-us", "/about_us", "/our-story", "/who-we-are"]
SERVICES_PATHS = [
    "/services",
    "/our-services",
    "/menu",
    "/products",
    "/what-we-do",
    "/offerings",
    "/work",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _row_get(row: dict, *keys: str) -> str:
    for k in keys:
        if k not in row:
            continue
        v = row.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() not in ("nan", "none", "null"):
            return s
    return ""


def _http_ok(status: int) -> bool:
    return 200 <= status < 400


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class BusinessProfile:
    name: str
    website: str
    location: str
    business_type: str

    page_title: str = ""
    meta_description: str = ""
    hero_text: str = ""
    headings: list = field(default_factory=list)
    about_text: str = ""
    services: list = field(default_factory=list)
    differentiators: list = field(default_factory=list)
    tone_signals: list = field(default_factory=list)
    cta_text: str = ""
    phone: str = ""
    address_scraped: str = ""
    promotions: list = field(default_factory=list)

    pages_scraped: list = field(default_factory=list)
    used_playwright: bool = False
    scrape_error: str = ""


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

DIFFERENTIATOR_PATTERNS = re.compile(
    r"(family[- ]owned|locally[- ]owned|since \d{4}|est\.?\s*\d{4}|"
    r"award[- ]winning|#1|number one|certified|licensed|insured|"
    r"free \w+|same[- ]day|24[/ ]7|satisfaction guaranteed)",
    re.IGNORECASE,
)

TONE_WORDS = {
    "casual": ["hey", "we're", "we love", "come on in", "stop by", "y'all", "awesome"],
    "formal": [
        "we provide",
        "our firm",
        "our professionals",
        "we specialize",
        "committed to excellence",
    ],
    "playful": ["yummy", "delicious", "fun", "amazing", "incredible", "wow"],
    "community": ["local", "neighborhood", "community", "neighbors", "serving"],
}

PHONE_RE = re.compile(r"(\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4})")

CTA_RE = re.compile(
    r"(call (us|now|today)|book (now|online|today)|get a (free )?quote|"
    r"contact us|schedule (a |an )?(free )?consultation|order (now|online)|"
    r"visit us|come (in|see us)|sign up|learn more|get started)",
    re.IGNORECASE,
)

PROMO_RE = re.compile(
    r"(\d+%\s*off|\$\d+\s+off|buy\s+\d+\s+get\s+\d+|free \w+|"
    r"limited time|special offer|discount|coupon|deal)",
    re.IGNORECASE,
)


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def extract_text(soup: BeautifulSoup, selector: str, limit: int = 1) -> list[str]:
    tags = soup.select(selector)[:limit]
    return [clean(t.get_text()) for t in tags if t.get_text(strip=True)]


def parse_page(html: str, url: str, profile: BusinessProfile, page_role: str) -> None:
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["nav", "footer", "script", "style", "noscript", "header"]):
        tag.decompose()

    body_text = clean(soup.get_text(separator=" "))

    if page_role == "home":
        if not profile.page_title:
            title_tag = soup.find("title")
            if title_tag:
                profile.page_title = clean(title_tag.get_text())

        if not profile.meta_description:
            meta = soup.find("meta", attrs={"name": "description"})
            if meta and meta.get("content"):
                profile.meta_description = clean(str(meta["content"]))

        h1s = extract_text(soup, "h1", limit=2)
        if h1s:
            profile.hero_text = h1s[0]

        profile.headings = extract_text(soup, "h2, h3", limit=8)

    elif page_role == "about":
        paragraphs = [
            clean(p.get_text()) for p in soup.find_all("p") if len(p.get_text(strip=True)) > 60
        ]
        profile.about_text = " ".join(paragraphs[:4])

    elif page_role == "services":
        items = []
        for tag in soup.find_all(["h2", "h3", "h4", "li"]):
            text = clean(tag.get_text())
            if 5 < len(text) < 80:
                items.append(text)
        profile.services = list(dict.fromkeys(items))[:15]

    found = DIFFERENTIATOR_PATTERNS.findall(body_text)
    profile.differentiators = list(
        dict.fromkeys(profile.differentiators + [d.lower() for d in found])
    )[:10]

    text_lower = body_text.lower()
    for tone, words in TONE_WORDS.items():
        if any(w in text_lower for w in words):
            if tone not in profile.tone_signals:
                profile.tone_signals.append(tone)

    if not profile.cta_text:
        cta_match = CTA_RE.search(body_text)
        if cta_match:
            profile.cta_text = clean(cta_match.group(0))

    if not profile.phone:
        phone_match = PHONE_RE.search(body_text)
        if phone_match:
            profile.phone = phone_match.group(0).strip()

    promos = PROMO_RE.findall(body_text)
    profile.promotions = list(dict.fromkeys(profile.promotions + [p.lower() for p in promos]))[:5]


# ---------------------------------------------------------------------------
# Playwright fallback
# ---------------------------------------------------------------------------


async def fetch_with_playwright(url: str) -> str | None:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.warning(
            "Playwright not installed -- skipping JS fallback. "
            "Run: pip install playwright && playwright install chromium"
        )
        return None

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                page = await browser.new_page(extra_http_headers=HEADERS)
                await page.goto(
                    url,
                    timeout=PLAYWRIGHT_TIMEOUT * 1000,
                    wait_until="domcontentloaded",
                )
                await page.wait_for_timeout(2000)
                return await page.content()
            finally:
                await browser.close()
    except Exception as e:  # noqa: BLE001
        log.debug("Playwright failed for %s: %s", url, e)
        return None


# ---------------------------------------------------------------------------
# HTTP fetcher
# ---------------------------------------------------------------------------


async def fetch(client: httpx.AsyncClient, url: str) -> tuple[str, int]:
    try:
        resp = await client.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        return resp.text, resp.status_code
    except Exception:
        return "", 0


async def probe_secondary_page(
    client: httpx.AsyncClient, base_url: str, candidates: list[str]
) -> tuple[str | None, str | None]:
    for path in candidates:
        url = urljoin(base_url, path)
        html, status = await fetch(client, url)
        if status == 200 and len(html) > 500:
            return html, url
    return None, None


# ---------------------------------------------------------------------------
# Per-business scrape
# ---------------------------------------------------------------------------


async def scrape_business(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    profile: BusinessProfile,
) -> BusinessProfile:
    async with semaphore:
        website = profile.website.strip()
        if not website.startswith("http"):
            website = "https://" + website

        parsed = urlparse(website)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        html, status = await fetch(client, base_url)

        if not _http_ok(status) or not html.strip():
            if not parsed.netloc.startswith("www."):
                alt = base_url.replace("://", "://www.", 1)
                html, status = await fetch(client, alt)
                if _http_ok(status) and html.strip():
                    base_url = alt

        if not _http_ok(status) or not html.strip():
            profile.scrape_error = "unreachable"
            return profile

        body_text = BeautifulSoup(html, "html.parser").get_text(strip=True)
        if len(body_text) < JS_FALLBACK_THRESHOLD:
            pw_html = await fetch_with_playwright(base_url)
            if pw_html:
                html = pw_html
                profile.used_playwright = True

        parse_page(html, base_url, profile, "home")
        profile.pages_scraped.append(base_url)

        about_html, about_url = await probe_secondary_page(client, base_url, ABOUT_PATHS)
        if about_html:
            parse_page(about_html, about_url, profile, "about")
            profile.pages_scraped.append(about_url)

        services_html, services_url = await probe_secondary_page(client, base_url, SERVICES_PATHS)
        if services_html:
            parse_page(services_html, services_url, profile, "services")
            profile.pages_scraped.append(services_url)

        return profile


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------


def load_businesses(
    input_file: str,
    data_row: int | None = None,
    first_data_rows: int | None = None,
) -> list[BusinessProfile]:
    """data_row: only that 1-based data row. first_data_rows: only rows 1..N after header."""
    businesses: list[BusinessProfile] = []
    with open(input_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            if first_data_rows is not None and i > first_data_rows:
                break
            if data_row is not None and i != data_row:
                continue
            businesses.append(
                BusinessProfile(
                    name=_row_get(row, "name", "Business Name"),
                    website=_row_get(row, "website", "Website"),
                    location=_row_get(row, "location", "Location (address)"),
                    business_type=_row_get(
                        row, "business_type", "What they do (primary category)"
                    ),
                )
            )
    return businesses


async def run(
    input_file: str,
    output_file: str,
    failed_file: str,
    limit: int | None = None,
    data_row: int | None = None,
    first_data_rows: int | None = None,
) -> None:
    businesses = load_businesses(
        input_file,
        data_row=data_row,
        first_data_rows=first_data_rows,
    )
    with_website = [b for b in businesses if b.website.strip()]
    if data_row is not None and businesses and not with_website:
        log.warning(
            "Data row %s (%s) has no Website in the CSV — nothing to scrape.",
            data_row,
            businesses[0].name,
        )
    skipped = len(businesses) - len(with_website)
    valid = with_website
    if limit is not None:
        valid = valid[: max(0, limit)]
    if skipped:
        log.info("Skipping %s businesses with no website", skipped)

    log.info("Scraping %s businesses (concurrency=%s)", len(valid), MAX_CONCURRENT)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    out_path = Path(output_file)
    fail_path = Path(failed_file)

    already_done: set[str] = set()
    if out_path.exists():
        with open(out_path, encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    already_done.add(obj.get("website", ""))
                except Exception:
                    pass
        log.info("Resuming -- %s already scraped", len(already_done))

    to_scrape = [b for b in valid if b.website not in already_done]
    log.info("%s remaining to scrape", len(to_scrape))

    if not to_scrape:
        log.info("Nothing to do.")
        return

    start = time.monotonic()
    done = 0

    async with httpx.AsyncClient(headers=HEADERS, http2=True) as client:
        tasks = [scrape_business(client, semaphore, b) for b in to_scrape]

        with (
            open(out_path, "a", encoding="utf-8") as out_f,
            open(fail_path, "a", encoding="utf-8") as fail_f,
        ):
            for coro in asyncio.as_completed(tasks):
                profile = await coro
                done += 1

                if profile.scrape_error:
                    fail_f.write(json.dumps(asdict(profile), ensure_ascii=False) + "\n")
                    fail_f.flush()
                    log.warning(
                        "[%s/%s] FAILED  %s -- %s",
                        done,
                        len(to_scrape),
                        profile.name,
                        profile.scrape_error,
                    )
                else:
                    out_f.write(json.dumps(asdict(profile), ensure_ascii=False) + "\n")
                    out_f.flush()
                    log.info("[%s/%s] OK  %s", done, len(to_scrape), profile.name)

    elapsed = time.monotonic() - start
    log.info(
        "Done. %s processed in %.0fs (%.1fs avg)",
        done,
        elapsed,
        elapsed / max(done, 1),
    )
    log.info("Results  -> %s", out_path)
    log.info("Failures -> %s", fail_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape business websites to JSONL profiles.")
    parser.add_argument(
        "input_file",
        nargs="?",
        default=INPUT_FILE,
        help=f"Input CSV (default: {INPUT_FILE})",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=OUTPUT_FILE,
        help=f"Success JSONL (default: {OUTPUT_FILE})",
    )
    parser.add_argument(
        "-f",
        "--failed",
        default=FAILED_FILE,
        help=f"Failed JSONL (default: {FAILED_FILE})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Scrape at most N businesses (after skipping rows with no website), in CSV order",
    )
    parser.add_argument(
        "--data-row",
        type=int,
        default=None,
        metavar="N",
        help="Only load CSV data row N (1 = first business row after header, 2 = second, …)",
    )
    parser.add_argument(
        "--first-data-rows",
        type=int,
        default=None,
        metavar="N",
        help="Only load the first N data rows (excludes header); row 1 = first business line",
    )
    args = parser.parse_args()
    inp = args.input_file
    if not Path(inp).is_file():
        log.error("Input file not found: %s", inp)
        sys.exit(1)
    asyncio.run(
        run(
            inp,
            args.output,
            args.failed,
            limit=args.limit,
            data_row=args.data_row,
            first_data_rows=args.first_data_rows,
        )
    )


if __name__ == "__main__":
    main()
