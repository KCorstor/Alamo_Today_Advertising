"""One-off run: businesses near a town via Places API (New) Nearby Search."""
import argparse
import os
import re
import time

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

TOWN = "Alamo, California"
OUT_CSV = "alamo, ca_businesses.csv"


def default_output_csv(location: str) -> str:
    """Filename for CSV when --output is omitted (keeps legacy name for default Alamo)."""
    if location.strip() == TOWN:
        return OUT_CSV
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", location.strip()).strip("_").lower()
    return f"{slug}_businesses.csv"

# Nearby Search (New) returns at most 20 places per request (no pagination).
# DISTANCE ranking + a denser grid reduces overlap vs a single POPULARITY top-20 per cell.
NEARBY_URL = "https://places.googleapis.com/v1/places:searchNearby"
# websiteUri uses Nearby Search Enterprise SKU; see field mask docs.
FIELD_MASK = (
    "places.id,"
    "places.displayName,"
    "places.primaryType,"
    "places.primaryTypeDisplayName,"
    "places.types,"
    "places.formattedAddress,"
    "places.websiteUri"
)


def geocode_town(town_name: str, api_key: str):
    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": town_name, "key": api_key}
    response = requests.get(geocode_url, params=params, timeout=30)
    if response.status_code != 200:
        print("Geocoding failed with status:", response.status_code)
        return None, None
    result = response.json()
    if not result.get("results"):
        print("No results found for town:", town_name)
        return None, None
    loc = result["results"][0]["geometry"]["location"]
    return loc["lat"], loc["lng"]


def search_nearby_new(api_key: str, lat: float, lng: float, radius_m: float):
    body = {
        "locationRestriction": {
            "circle": {
                "center": {"latitude": lat, "longitude": lng},
                "radius": float(radius_m),
            }
        },
        "maxResultCount": 20,
        "rankPreference": "DISTANCE",
    }
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": FIELD_MASK,
    }
    response = requests.post(NEARBY_URL, json=body, headers=headers, timeout=30)
    if response.status_code != 200:
        print(f"searchNearby error {response.status_code}: {response.text[:500]}")
        return None
    data = response.json()
    if "error" in data:
        print("searchNearby API error:", data.get("error"))
        return None
    return data


def _localized_text(obj):
    if not obj:
        return ""
    if isinstance(obj, dict):
        return (obj.get("text") or "").strip()
    return str(obj)


def row_from_place(place: dict):
    display = place.get("displayName") or {}
    primary_label = _localized_text(place.get("primaryTypeDisplayName"))
    primary_code = place.get("primaryType") or ""
    types_list = place.get("types") or []
    what_they_do = primary_label or primary_code.replace("_", " ") or ""

    return {
        "Business Name": display.get("text"),
        "What they do (primary category)": what_they_do,
        "All place types": ", ".join(types_list),
        "Location (address)": place.get("formattedAddress"),
        "Website": (place.get("websiteUri") or "").strip(),
        "Place ID": place.get("id"),
    }


def get_all_places_cell(api_key: str, lat: float, lng: float, radius_m: float):
    data = search_nearby_new(api_key, lat, lng, radius_m)
    if not data:
        return []
    return [row_from_place(p) for p in data.get("places") or []]


def search_multiple_areas(
    api_key,
    base_lat,
    base_lng,
    radius,
    grid_size=7,
    lat_step=0.02,
    lng_step=0.02,
):
    """grid_size x grid_size cells centered on (base_lat, base_lng); step in degrees."""
    all_places = []
    for i in range(grid_size):
        for j in range(grid_size):
            lat = base_lat + (i - grid_size // 2) * lat_step
            lng = base_lng + (j - grid_size // 2) * lng_step
            print(f"searchNearby at {lat:.6f},{lng:.6f} (r={radius}m)")
            all_places.extend(get_all_places_cell(api_key, lat, lng, radius))
            time.sleep(0.05)
    return all_places


def main(
    limit: int | None = None,
    location: str = TOWN,
    output_csv: str | None = None,
    grid_size: int = 7,
    lat_step: float = 0.02,
    lng_step: float = 0.02,
):
    api_key = os.environ.get("GOOGLE_PLACE_API_KEY")
    if not api_key:
        raise SystemExit("Set GOOGLE_PLACE_API_KEY in your environment or .env file.")

    base_lat, base_lng = geocode_town(location, api_key)
    if base_lat is None:
        raise SystemExit("Geocoding failed.")

    out_path = output_csv or default_output_csv(location)

    if grid_size < 1:
        raise SystemExit("--grid-size must be at least 1")

    radius = 2000

    if limit is not None:
        cap = max(1, min(limit, 20))
        print(
            f"Places API (New) single search at {location} center "
            f"(max {cap} results, DISTANCE rank, r={radius}m)..."
        )
        rows = get_all_places_cell(api_key, base_lat, base_lng, radius)[:cap]
    else:
        n_cells = grid_size * grid_size
        print(
            f"Places API (New) Nearby Search near {location} "
            f"(max 20/cell, DISTANCE rank, {grid_size}x{grid_size} grid, "
            f"{n_cells} API calls, step {lat_step}/{lng_step} deg)..."
        )
        rows = search_multiple_areas(
            api_key,
            base_lat,
            base_lng,
            radius,
            grid_size=grid_size,
            lat_step=lat_step,
            lng_step=lng_step,
        )

    by_id = {}
    for row in rows:
        pid = row.get("Place ID")
        if pid:
            by_id[pid] = row
    unique = list(by_id.values())
    print(f"Total rows (deduped by Place ID): {len(unique)}")

    df = pd.DataFrame(unique)
    cols = [
        "Business Name",
        "What they do (primary category)",
        "All place types",
        "Location (address)",
        "Website",
        "Place ID",
    ]
    df = df[[c for c in cols if c in df.columns]]
    df.to_csv(out_path, index=False)
    print(f"Saved {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Nearby Search export around a geocoded location (default: Alamo, CA)."
    )
    parser.add_argument(
        "--location",
        default=TOWN,
        metavar="ADDRESS",
        help='Area to geocode, e.g. "Danville, California" (default: Alamo, California)',
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        dest="output_csv",
        metavar="FILE",
        help="Output CSV path (default: alamo, ca_businesses.csv for Alamo; else slug_businesses.csv)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only run one search at the town center and keep the first N rows (N≤20; API max per call).",
    )
    parser.add_argument(
        "--grid-size",
        type=int,
        default=7,
        metavar="N",
        help="N×N grid of search centers (default: 7). Larger = more coverage, more API calls.",
    )
    parser.add_argument(
        "--lat-step",
        type=float,
        default=0.02,
        metavar="DEG",
        help="Degrees between grid rows (default: 0.02 ≈ 2.2 km).",
    )
    parser.add_argument(
        "--lng-step",
        type=float,
        default=0.02,
        metavar="DEG",
        help="Degrees between grid columns (default: 0.02).",
    )
    args = parser.parse_args()
    main(
        limit=args.limit,
        location=args.location,
        output_csv=args.output_csv,
        grid_size=args.grid_size,
        lat_step=args.lat_step,
        lng_step=args.lng_step,
    )
