"""
Capture a viewport (or full-page) screenshot per business website from the CSV.

Requires: pip install playwright && playwright install chromium

Respect site terms, robots.txt, and reasonable rate limits.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import re
import sys
from pathlib import Path

INPUT_CSV = "alamo, ca_businesses.csv"
OUT_DIR = "website_screenshots"
VIEWPORT_W = 1280
VIEWPORT_H = 720
NAV_TIMEOUT_MS = 35_000
SETTLE_MS = 1_500
MAX_CONCURRENT = 3


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


def _normalize_url(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    return raw


def _safe_filename(name: str, url: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_")[:60]
    if not slug:
        slug = "site"
    h = hashlib.sha256(url.encode()).hexdigest()[:10]
    return f"{slug}_{h}.png"


def load_targets(path: Path, limit: int | None) -> list[tuple[str, str]]:
    """Return list of (business_name, website_url)."""
    rows: list[tuple[str, str]] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = _row_get(row, "name", "Business Name")
            web = _normalize_url(_row_get(row, "website", "Website"))
            if not web:
                continue
            rows.append((name, web))
            if limit is not None and len(rows) >= limit:
                break
    return rows


async def screenshot_one(
    browser,
    semaphore: asyncio.Semaphore,
    name: str,
    url: str,
    out_dir: Path,
    full_page: bool,
    width: int,
    height: int,
) -> tuple[str, str]:
    """Returns (url, status) where status is 'ok' or error message."""
    async with semaphore:
        context = await browser.new_context(
            viewport={"width": width, "height": height},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()
        path = out_dir / _safe_filename(name, url)
        try:
            await page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
            await page.wait_for_timeout(SETTLE_MS)
            await page.screenshot(path=str(path), full_page=full_page, type="png")
            return url, "ok"
        except Exception as e:  # noqa: BLE001
            return url, str(e)
        finally:
            await context.close()


async def run_all(
    targets: list[tuple[str, str]],
    out_dir: Path,
    full_page: bool,
    width: int,
    height: int,
) -> None:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print(
            "Install Playwright: pip install playwright && playwright install chromium",
            file=sys.stderr,
        )
        sys.exit(1)

    out_dir.mkdir(parents=True, exist_ok=True)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            tasks = [
                screenshot_one(browser, semaphore, name, url, out_dir, full_page, width, height)
                for name, url in targets
            ]
            results = await asyncio.gather(*tasks)
        finally:
            await browser.close()

    ok = sum(1 for _, s in results if s == "ok")
    print(f"Saved {ok}/{len(results)} screenshots under {out_dir.resolve()}")
    for url, status in results:
        if status != "ok":
            print(f"  FAIL {url}  {status}", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser(description="Screenshot each website from the business CSV.")
    ap.add_argument("csv_path", nargs="?", default=INPUT_CSV, help="Input CSV path")
    ap.add_argument("-o", "--out-dir", default=OUT_DIR, help="Output directory for PNGs")
    ap.add_argument("--width", type=int, default=VIEWPORT_W)
    ap.add_argument("--height", type=int, default=VIEWPORT_H)
    ap.add_argument(
        "--full-page",
        action="store_true",
        help="Capture full scrollable page (taller images; slower)",
    )
    ap.add_argument("--limit", type=int, default=None, metavar="N", help="Max N sites with URLs")
    args = ap.parse_args()

    csv_path = Path(args.csv_path)
    if not csv_path.is_file():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    targets = load_targets(csv_path, args.limit)
    if not targets:
        print("No rows with a Website column; nothing to do.", file=sys.stderr)
        sys.exit(0)

    asyncio.run(
        run_all(
            targets,
            Path(args.out_dir),
            full_page=args.full_page,
            width=args.width,
            height=args.height,
        )
    )


if __name__ == "__main__":
    main()
