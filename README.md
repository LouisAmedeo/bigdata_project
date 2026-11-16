# Big Data project — Ponctualité des TGV (2018-2026)

## Authors

- AMEDEO, Louis
- <NOM>, <Prénom>

## Prerequisites

- Compte Onyxia (`onyxia.adm.adaltas.cloud`) et service `vscode-pyspark`
- Accès S3 configuré (profil AWS `default`)
- <autres outils / versions>

## Dataset

| Champ | Valeur |
|---|---|
| Nom | Régularité mensuelle TGV par liaisons (SNCF Voyageurs / AQST) |
| URL | https://ressources.data.sncf.com/explore/dataset/regularite-mensuelle-tgv-aqst/ |
| Licence | ODbL (Open Database License) |
| Date de téléchargement | 2026-11-16 |
| Taille (brute) | 2 909 048 octets (2,9 Mo), CSV `;`, 12 907 lignes × 26 colonnes |
| Version | extraction figée `date <= 2026-09` (2018-01 → 2026-09), triée par date, service, gares |
| sha256 | `8b2f5a27593f32516f0781ab591d8316f9b09e639fa5f4d76496046e837602ee` |

## Problem statement

Entre 2018-2019 (avant Covid) et 2024-2025, quelles liaisons TGV ont le plus perdu en ponctualité à l'arrivée, et quelle famille de causes explique cette dégradation ? Le pipeline produit un classement des liaisons par variation du taux de trains à l'heure, avec la répartition des causes de retard. Le résultat est vérifié par un recalcul indépendant sous DuckDB à partir du CSV brut.

## S3 paths

| Couche | Chemin |
|---|---|
| Raw | `s3://user-l-amedeo-ece/bigdata_project/raw/regularite_tgv/extraction=2026-09/regularite-mensuelle-tgv-aqst.csv` (+ `.meta`) |
| Processed | `s3://user-l-amedeo-ece/bigdata_project/processed/` |

Bucket de référence : `user-l-amedeo-ece`. Les scripts utilisent `BUCKET=${BUCKET:-$KUBERNETES_NAMESPACE}`, donc chaque compte Onyxia écrit dans son propre bucket.

## Reproduction

1. Cloner le dépôt : `git clone https://github.com/LouisAmedeo/bigdata_project.git && cd bigdata_project`
2. Téléchargement des données brutes : `bash src/00_download.sh` (écrit le CSV et un fichier `.meta` avec taille et sha256 dans `data/raw/`)
3. Ingestion raw sur S3 par un Job Kubernetes (télécharge à la source, vérifie le sha256, dépose sur S3) : `bash src/01_ingest_raw.sh` (manifeste `k8s/01-job-ingest-raw.yaml`)
4. <exécution du pipeline>
5. <vérification du résultat>

**Relance / reprise :** <comment relancer une étape en cas d'échec>
