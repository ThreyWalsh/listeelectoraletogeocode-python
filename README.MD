# Script Geolisting à destination de UMap(OpenStreetMap)

Ce script prend en entrée un fichier CSV des listes électorales, et le convertit en fichier GeoJson à utiliser directement sur OpenStreetMap, Google Earth, etc.

## Prérequis
- Installation de Python3

- Installation des librairies requests, tqdm et geopy

```bash
pip install requests tqdm geopy
```

## Instructions
Lancer la commande suivante
```bash
python csv_to_umap_geojson.py --input "ListingElectoral.csv"
```

## Arguments
| Argument  | Raccourci | Requis | Description | Valeur par défaut |
| --- | --- | --- | --- | --- |
| --input | -i | ✅ | CSV d'entrée | Aucune |
| --outdir | Aucun | ❌ | Répertoire parent des résultats | results |
| --pause | Aucun | ❌ | Pause(s) entre requêtes Nominatim | 1 | 
| --user-agent | Aucun | ❌ | User-Agent ou email pour Nominatim | csv_to_umap_script |
| --limit | Aucun | ❌ | Limiter le nombre de lignes (pour test) | Aucune |

## Fichiers de sortie
- Dans /results
| Fichier  | Rôle | Contenu au fil du temps |
| --- | --- | --- |
| geocache.json | Cache « classique » : mémorise toutes les adresses déjà géocodées (anciennes et nouvelles) pour éviter de refaire des requêtes Nominatim. | → À chaque exécution : on relit l’existant;on ajoute les nouvelles adresses rencontrées dans le CSV courant; il grandit uniquement selon les adresses que vous traitez (pas celles que vous n’avez jamais dans vos CSV). |
| geocache_new.json | Cache cumulatif des nouveautés : sert de « journal » de toutes les adresses jamais vues comme nouvelles lors d’une exécution. | → Il ne se contente pas du CSV actuel : il garde en mémoire chaque adresse qui a été ajoutée un jour au cache, même si elle avait déjà été rencontrée dans un run précédent; il constitue l’historique de tous les ajouts successifs. |

- Dans les dossiers horodatés:
| Fichier  | Contenu |
| --- | --- |
| output_umap.geojson | Points géocodés |
| output_not_geocoded.geojson | Adresses introuvables |
| output_incomplete.geojson | Adresses incomplètes |
| output_duplicates.geojson | Doublons |
| geocache_added.json | Nouvelles entrées ajoutées au cache global lors de CE RUN |
| geocache_new_added.json | Nouvelles entrées ajoutées au cache cumulatif lors de CE RUN |
| quality_report.csv | Statistiques globales |
| problematic_rows.csv | Listing de toutes les lignes problématiques |