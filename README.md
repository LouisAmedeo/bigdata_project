# Big Data project — Ponctualité des TGV

[![verif](https://github.com/LouisAmedeo/bigdata_project/actions/workflows/verif.yml/badge.svg)](https://github.com/LouisAmedeo/bigdata_project/actions/workflows/verif.yml)

## Authors

AMEDEO Louis, BENATAR Arthur

## Prerequisites

Compte Onyxia Adaltas, service **vscode-pyspark** par défaut (`kubectl`, `aws`, `duckdb`, Spark 4.1.1, identifiants S3 `AWS_*`), accès Internet.

## Dataset

- **Régularité mensuelle TGV par liaisons** (SNCF Voyageurs / AQST), licence **ODbL** : https://ressources.data.sncf.com/explore/dataset/regularite-mensuelle-tgv-aqst/
- Téléchargé le 2026-11-16 (extraction figée `date <= 2026-09`) ; 2 909 048 octets (2,9 Mo), CSV `;`, 12 907 lignes × 26 colonnes, sha256 `8b2f5a27…7602ee`

## Problem statement

Entre 2018-2019 et 2024-2025, quelles liaisons TGV ont le plus perdu en ponctualité à l'arrivée, et pour quelles causes ? Le pipeline classe les liaisons par variation du taux de trains à l'heure, avec la contribution de chaque cause de retard. Le résultat est recalculé indépendamment sous DuckDB à partir du CSV brut.

## S3 paths

Bucket `$KUBERNETES_NAMESPACE`, ici `user-a-benatar-ece` : raw `s3://user-a-benatar-ece/bigdata_project/raw/regularite_tgv/extraction=2026-09/` (CSV + `.meta`) ; processed `s3://user-a-benatar-ece/bigdata_project/processed/` (`silver/`, `gold/`, `quality/`, `verification/`, `benchmark/`).

## Reproduction

Dans le terminal du service : `git clone https://github.com/LouisAmedeo/bigdata_project.git && cd bigdata_project`, puis `bash src/run_pipeline.sh` (≈ 3 min) qui enchaîne :

- étape 2 : `bash src/01_ingest_raw.sh` — Job k8s, brut vérifié (sha256) vers `raw/` ;
- étape 3 : `bash src/02_transform.sh` — Job k8s Spark : qualité, silver, gold ;
- étape 4 : `bash src/03_verify.sh` — DuckDB depuis le brut : `VERIFICATION OK`.

**Relance :** étapes idempotentes, relancer suffit ; reprise à l'étape N : `bash src/run_pipeline.sh N` ; échec : `kubectl logs job/<nom>`. Mesures du rapport : `bash src/04_benchmark.sh` ; vérif à la main du n°1 (Python pur, brut local) : `python3 src/05_verif_manuelle.py`.
