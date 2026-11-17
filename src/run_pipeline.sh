#!/usr/bin/env bash
# Enchaîne tout le pipeline sur Onyxia, depuis la racine du dépôt :
#   bash src/run_pipeline.sh        # étapes 2 -> 4 (ingestion, Spark, vérification DuckDB)
#   bash src/run_pipeline.sh 3      # reprise à l'étape 3 si l'ingestion est déjà faite
#   bash src/run_pipeline.sh 4      # seulement la vérification
# Chaque étape est idempotente : on peut relancer n'importe laquelle autant de fois que voulu.
# (L'étape 1, src/00_download.sh, sert à télécharger le CSV en local ; le Job de l'étape 2 le
#  télécharge lui-même à la source et vérifie son sha256.)

set -euo pipefail
cd "$(dirname "$0")/.."
FROM="${1:-2}"
T0=$(date +%s)
if [ "$FROM" -le 2 ]; then bash src/01_ingest_raw.sh; fi
if [ "$FROM" -le 3 ]; then bash src/02_transform.sh; fi
if [ "$FROM" -le 4 ]; then bash src/03_verify.sh; fi
echo "== pipeline terminé en $(( $(date +%s) - T0 )) s | $(date '+%F %T %Z')"
