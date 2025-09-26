#!/usr/bin/env python3
"""
csv_to_umap_geojson.py 

Usage :
    python csv_to_umap_geojson.py --input "ListingElectoral.csv" [--limit 100]

Fonctionnalités :
 - GeoJSON 100% compatible uMap (FeatureCollection, coordonnées [lon,lat], UTF-8)
 - Géocodage Nominatim avec fallback BAN (api-adresse.data.gouv.fr)
 - Tentative sans numéro si l’adresse complète échoue
 - Caches persistants
"""

import argparse, csv, json, datetime, re
from pathlib import Path
from tqdm import tqdm
import requests
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# ----------------------- UTILITAIRES ----------------------- #
def build_address(row):
    keys = ["NumeroVoie","LibelleVoie","Complement1","Complement2",
            "LieuDit","CodePostal","CommuneAdresse","Pays"]
    parts = []
    missing = []
    for k in keys:
        v = row.get(k, "")
        if v is None or str(v).strip() == "":
            missing.append(k)
        else:
            parts.append(str(v).strip())
    return ", ".join(parts), missing

def geocode_nominatim(address, geocode):
    try:
        res = geocode(address)
        if res:
            return float(res.latitude), float(res.longitude)
    except Exception:
        pass
    return None

def geocode_ban(address):
    """Fallback géocodage Base Adresse Nationale (France)."""
    try:
        r = requests.get(
            "https://api-adresse.data.gouv.fr/search/",
            params={"q": address, "limit": 1},
            timeout=8
        )
        js = r.json()
        if js.get("features"):
            lon, lat = js["features"][0]["geometry"]["coordinates"]
            return float(lat), float(lon)
    except Exception:
        pass
    return None

def geocode_address(address, geocode):
    # 1) Nominatim
    res = geocode_nominatim(address, geocode)
    if res: return res
    # 2) Sans numéro
    addr_wo = re.sub(r"^\d+\s+", "", address)
    if addr_wo != address:
        res = geocode_nominatim(addr_wo, geocode)
        if res: return res
    # 3) BAN
    return geocode_ban(address)

def make_feature(lon, lat, name, desc):
    """Feature strictement valide pour uMap"""
    return {
        "type": "Feature",
        "geometry": {"type":"Point","coordinates":[lon, lat]},
        "properties": {
            "name": name,
            "description": desc,
            "_umap_options": {"color": "blue"}
        }
    }

# ------------------------------------------------------------------ #
def main(input_csv: Path, outdir: Path, limit: int|None):
    outdir.mkdir(parents=True, exist_ok=True)

    # Détection du séparateur
    with open(input_csv,"r",encoding="utf-8") as f:
        first = f.readline()
    delimiter = ";" if first.count(";") > first.count(",") else ","

    geolocator = Nominatim(user_agent="csv_to_umap_script")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    with open(input_csv,newline="",encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        rows = list(reader)

    if limit:
        rows = rows[:limit]

    # --- Caches ---
    cache_file       = outdir.parent / "geocache.json"
    new_cache_global = outdir.parent / "geocache_new.json"
    cache = {}
    new_global = {}
    if cache_file.exists():
        try:
            cache = json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            cache = {}
    if new_cache_global.exists():
        try:
            new_global = json.loads(new_cache_global.read_text(encoding="utf-8"))
        except Exception:
            new_global = {}

    geocache_added, geocache_new_added = {}, {}

    geocoded, not_geocoded = [], []
    incomplete, duplicates = [], []
    problematic_rows = []
    seen_addresses = {}

    for r in tqdm(rows, desc="Géocodage"):
        address, missing = build_address(r)
        if not address:
            continue
        name = (r.get("NomUsage") or r.get("NomNaissance") or "").strip()
        desc = f"{name} | Adresse : {address}"

        reasons = []
        if missing:
            reasons.append("incomplete")
            incomplete.append(make_feature(0, 0, name, desc))

        lat = lon = None
        if address in cache:
            val = cache[address]
            # rétrocompatibilité : ancien dict ou nouveau [lat, lon]
            if isinstance(val, dict):
                lat, lon = val.get("lat"), val.get("lon")
            elif isinstance(val, (list, tuple)) and len(val) >= 2:
                lat, lon = val[0], val[1]

        if lat is None or lon is None:
            coords = geocode_address(address, geocode)
            if coords:
                lat, lon = coords
                cache[address] = [lat, lon]
                geocache_added[address] = [lat, lon]
                if address not in new_global:
                    new_global[address] = [lat, lon]
                    geocache_new_added[address] = [lat, lon]
                cache_file.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
                new_cache_global.write_text(json.dumps(new_global, ensure_ascii=False, indent=2), encoding="utf-8")

        if lat is None or lon is None:
            reasons.append("not_geocoded")
            not_geocoded.append(make_feature(0, 0, name, desc))
        else:
            feat = make_feature(lon, lat, name, desc)
            geocoded.append(feat)
            if address in seen_addresses:
                reasons.append("duplicate")
                duplicates.append(feat)
            else:
                seen_addresses[address] = True

        if reasons:
            row_copy = r.copy()
            row_copy["reason"] = ";".join(reasons)
            problematic_rows.append(row_copy)

    # --- Sauvegardes ---
    def write_geojson(name, feats):
        p = outdir / name
        p.write_text(json.dumps({"type":"FeatureCollection","features":feats},
                                ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"✔️ {len(feats)} → {p}")

    write_geojson("output_umap.geojson", geocoded)
    write_geojson("output_not_geocoded.geojson", not_geocoded)
    write_geojson("output_incomplete.geojson", incomplete)
    write_geojson("output_duplicates.geojson", duplicates)

    # Rapport qualité
    quality_path = outdir / "quality_report.csv"
    quality_path.write_text(
        "total,geocoded,not_geocoded,incomplete,duplicates\n"
        f"{len(rows)},{len(geocoded)},{len(not_geocoded)},"
        f"{len(incomplete)},{len(duplicates)}\n",
        encoding="utf-8"
    )
    print(f"✔️ Rapport qualité → {quality_path}")

    # Lignes problématiques
    if problematic_rows:
        fieldnames = list(rows[0].keys()) + ["reason"]
        with open(outdir / "problematic_rows.csv","w",newline="",encoding="utf-8") as fw:
            writer = csv.DictWriter(fw, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(problematic_rows)
        print(f"✔️ Lignes problématiques → {outdir/'problematic_rows.csv'}")

    # Caches des nouvelles entrées de ce run
    if geocache_added:
        (outdir / "geocache_added.json").write_text(
            json.dumps(geocache_added, ensure_ascii=False, indent=2), encoding="utf-8")
    if geocache_new_added:
        (outdir / "geocache_new_added.json").write_text(
            json.dumps(geocache_new_added, ensure_ascii=False, indent=2), encoding="utf-8")

# ------------------------------------------------------------------ #
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", required=True, help="CSV d'entrée")
    p.add_argument("--outdir", default="results", help="Répertoire parent des résultats (défaut: results)")
    p.add_argument("--limit", type=int, help="Limiter le nombre de lignes pour test")
    args = p.parse_args()

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    main(Path(args.input), Path(args.outdir)/timestamp, args.limit)