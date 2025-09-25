#!/usr/bin/env python3
"""
csv_to_umap_geojson.py

Usage :
    python csv_to_umap_geojson.py --input "Listing SECLIN.csv"

Fonctions :
 - Création des GeoJSON (umap, not_geocoded, incomplete, duplicates)
 - Rapport qualité + CSV des lignes problématiques
 - Cache global geocache.json mis à jour
 - Cache cumulatif geocache_new.json mis à jour
 - ➕ Dans chaque dossier horodaté :
       * geocache_added.json      (nouvelles entrées ajoutées à geocache.json ce run)
       * geocache_new_added.json  (nouvelles entrées ajoutées à geocache_new.json ce run)
"""

import argparse, csv, json, time, datetime
from pathlib import Path
import requests
from tqdm import tqdm

try:
    from geopy.geocoders import Nominatim
    geopy_available = True
except Exception:
    geopy_available = False

def build_address(row):
    keys = ["NumeroVoie","LibelleVoie","Complement1","Complement2",
            "LieuDit","CodePostal","CommuneAdresse","Pays"]
    parts, missing = [], []
    for k in keys:
        v = row.get(k, "")
        if v is None or str(v).strip()=="":
            missing.append(k)
        else:
            parts.append(str(v).strip())
    return ", ".join(parts), missing

def load_cache(path):
    p = Path(path)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cache(cache, path):
    Path(path).write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

class NominatimGeocoder:
    def __init__(self, user_agent="csv_to_umap_script", pause=1.0):
        self.pause = pause
        self.user_agent = user_agent
        self.url = "https://nominatim.openstreetmap.org/search"
        if geopy_available:
            self.geolocator = Nominatim(user_agent=user_agent, timeout=10)
        else:
            self.geolocator = None
    def geocode(self, addr):
        if not addr: return None
        if self.geolocator:
            try:
                res = self.geolocator.geocode(addr, addressdetails=True)
                time.sleep(self.pause)
                if res:
                    return float(res.latitude), float(res.longitude), res.raw
            except Exception:
                pass
        # fallback HTTP
        params = {"q": addr, "format":"json","limit":1,"addressdetails":1}
        headers = {"User-Agent": self.user_agent}
        r = requests.get(self.url, params=params, headers=headers, timeout=15)
        time.sleep(self.pause)
        if r.ok:
            js = r.json()
            if js:
                try:
                    return float(js[0]["lat"]), float(js[0]["lon"]), js[0]
                except Exception:
                    return None
        return None

def make_feature(lon, lat, tooltip, popup, extra=None):
    props = {"name": tooltip, "description": popup}
    if extra: props.update(extra)
    return {
        "type":"Feature",
        "geometry":{"type":"Point","coordinates":[lon,lat]},
        "properties":props
    }

def csv_to_geojson(input_csv, outdir, pause=1.0, user_agent="csv_to_umap_script", limit=None):
    outdir.mkdir(parents=True, exist_ok=True)

    # Cache global et cumulatif
    cache_file       = outdir.parent / "geocache.json"
    new_cache_global = outdir.parent / "geocache_new.json"

    cache       = load_cache(cache_file)
    new_global  = load_cache(new_cache_global)
    geocache_added      = {}  # 🆕 ajoutés au cache global CE RUN
    geocache_new_added  = {}  # 🆕 ajoutés au cumul CE RUN

    geocoder = NominatimGeocoder(user_agent=user_agent, pause=pause)

    with open(input_csv, newline="", encoding="utf-8") as fh:
        sample = fh.read(4096); fh.seek(0)
        dialect = csv.Sniffer().sniff(sample) if sample else csv.excel
        reader = csv.DictReader(fh, dialect=dialect)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    total = len(rows) if limit is None else min(limit, len(rows))
    geocoded, not_geocoded, incomplete, duplicates = [], [], [], []
    problematic_rows = []
    address_seen = {}

    for row in tqdm(rows[:total], desc="Traitement", unit="ligne"):
        address, missing = build_address(row)
        tooltip = f"{(row.get('NomUsage') or row.get('NomNaissance') or '').strip()} {(row.get('Prenoms') or '').strip()}".strip()
        popup = f"<b>{tooltip}</b><br>Date de naissance : {(row.get('DateNaissance') or 'N/A').strip()}<br>Adresse : {address}"
        extra = {"full_address": address}
        reasons = []

        if missing:
            reasons.append("incomplete")
            incomplete.append(make_feature(0,0,tooltip,popup,extra))

        lat = lon = None
        if address in cache:
            lat, lon = cache[address]["lat"], cache[address]["lon"]
        else:
            res = geocoder.geocode(address)
            if res:
                lat, lon, raw = res
                cache[address] = {"lat":lat, "lon":lon, "raw":raw}
                geocache_added[address] = {"lat":lat, "lon":lon, "raw":raw}  # ➕ nouveau pour ce run
                # ajout au cumul s'il n'existait pas déjà
                if address not in new_global:
                    new_global[address] = {"lat":lat, "lon":lon, "raw":raw}
                    geocache_new_added[address] = {"lat":lat, "lon":lon, "raw":raw}
                save_cache(cache, cache_file)
                save_cache(new_global, new_cache_global)

        if lat is None or lon is None:
            reasons.append("not_geocoded")
            not_geocoded.append(make_feature(0,0,tooltip,popup,extra))
        else:
            feat = make_feature(lon,lat,tooltip,popup,extra)
            geocoded.append(feat)
            if address in address_seen:
                reasons.append("duplicate")
                duplicates.append(feat)
            else:
                address_seen[address] = True

        if reasons:
            problematic = row.copy()
            problematic["reason"] = ";".join(reasons)
            problematic_rows.append(problematic)

    def write_geojson(name, feats):
        path = outdir / name
        path.write_text(json.dumps({"type":"FeatureCollection","features":feats},
                                   ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"✅ {len(feats)} → {path}")

    write_geojson("output_umap.geojson", geocoded)
    write_geojson("output_not_geocoded.geojson", not_geocoded)
    write_geojson("output_incomplete.geojson", incomplete)
    write_geojson("output_duplicates.geojson", duplicates)

    # Rapport CSV synthétique
    (outdir / "quality_report.csv").write_text(
        "total,geocoded,not_geocoded,incomplete,duplicates\n"
        f"{total},{len(geocoded)},{len(not_geocoded)},{len(incomplete)},{len(duplicates)}\n",
        encoding="utf-8"
    )

    # CSV détaillé des lignes problématiques
    with open(outdir / "problematic_rows.csv", "w", newline="", encoding="utf-8") as fw:
        writer = csv.DictWriter(fw, fieldnames=fieldnames + ["reason"])
        writer.writeheader()
        writer.writerows(problematic_rows)
    print(f"⚠️ Lignes problématiques : {outdir/'problematic_rows.csv'} ({len(problematic_rows)} entrées)")

    # ➕ Sauvegarde des ajouts du run
    if geocache_added:
        save_cache(geocache_added, outdir / "geocache_added.json")
        print(f"🆕 {len(geocache_added)} nouvelles adresses → {outdir/'geocache_added.json'}")
    else:
        print("ℹ️ Aucune nouvelle entrée dans geocache.json ce run.")

    if geocache_new_added:
        save_cache(geocache_new_added, outdir / "geocache_new_added.json")
        print(f"🆕 {len(geocache_new_added)} nouvelles adresses → {outdir/'geocache_new_added.json'}")
    else:
        print("ℹ️ Aucune nouvelle entrée dans geocache_new.json ce run.")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input","-i",required=True,help="CSV d'entrée")
    p.add_argument("--outdir",default="results",help="Répertoire parent des résultats (défaut: results)")
    p.add_argument("--pause",type=float,default=1.0,help="Pause (s) entre requêtes Nominatim")
    p.add_argument("--user-agent",default="csv_to_umap_script",help="User-Agent ou email pour Nominatim")
    p.add_argument("--limit",type=int,help="Limiter le nombre de lignes (test)")
    a = p.parse_args()

    # Dossier horodaté
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(a.outdir) / timestamp
    csv_to_geojson(a.input, outdir, a.pause, a.user_agent, a.limit)