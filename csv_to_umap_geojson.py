#!/usr/bin/env python3
"""
csv_to_umap_geojson_umapfix.py  (rétrocompatible geocache)

Usage :
    python csv_to_umap_geojson_umapfix.py --input "Listing SECLIN.csv" [--limit 100]

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
    parts = [str(row.get(k,"")).strip() for k in keys if row.get(k)]
    return ", ".join([p for p in parts if p])

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

# ----------------------- TRAITEMENT ------------------------ #
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

    # --- Cache rétrocompatible ---
    cache_file = outdir.parent / "geocache.json"
    cache = {}
    if cache_file.exists():
        try:
            cache = json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            cache = {}

    features, not_geocoded = [], []

    for r in tqdm(rows, desc="Géocodage"):
        addr = build_address(r)
        if not addr:
            continue

        lat = lon = None
        if addr in cache:
            val = cache[addr]
            # ✅ Rétrocompatibilité : ancien format dict ou nouveau format liste
            if isinstance(val, dict):
                lat, lon = val.get("lat"), val.get("lon")
            elif isinstance(val, (list, tuple)) and len(val) >= 2:
                lat, lon = val[0], val[1]

        # Si pas trouvé ou invalide, on géocode
        if lat is None or lon is None:
            coords = geocode_address(addr, geocode)
            if coords:
                lat, lon = coords
                # Stockage au format liste (nouveau standard)
                cache[addr] = [lat, lon]
                cache_file.write_text(
                    json.dumps(cache, ensure_ascii=False, indent=2),
                    encoding="utf-8"
                )

        if lat is not None and lon is not None:
            feat = make_feature(lon, lat,
                                (r.get("NomUsage") or r.get("NomNaissance") or "").strip(),
                                addr)
            features.append(feat)
        else:
            not_geocoded.append(r)

    # --- Écriture des fichiers GeoJSON --- #
    def write_geojson(name, feats):
        path = outdir / name
        path.write_text(
            json.dumps({"type":"FeatureCollection","features":feats}, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        print(f"✔️ {len(feats)} → {path}")

    write_geojson("output_umap.geojson", features)
    write_geojson("output_not_geocoded.geojson", [])  # fichier vide mais valide

# ----------------------- POINT D'ENTRÉE --------------------- #
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", required=True, help="CSV d'entrée")
    p.add_argument("--outdir", default="results", help="Répertoire parent des résultats (défaut: results)")
    p.add_argument("--limit", type=int, help="Limiter le nombre de lignes pour test")
    args = p.parse_args()

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    main(Path(args.input), Path(args.outdir)/timestamp, args.limit)