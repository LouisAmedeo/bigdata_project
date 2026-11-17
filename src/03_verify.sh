#!/usr/bin/env bash
# Étape 4 : vérification indépendante du résultat sous DuckDB (CLI du service vscode-pyspark).
#   bash src/03_verify.sh
# DuckDB relit le CSV brut sur S3, recalcule le classement en SQL et le compare au gold Spark.
# Code retour != 0 si un écart est détecté. Relance : idempotente (sorties écrasées).

set -euo pipefail
cd "$(dirname "$0")/.."
. src/common.sh

banner "étape 4 : vérification indépendante DuckDB"
check_s3
duckdb --version
# le secret créé par le script a une portée s3://$BUCKET/ : il prime sur d'éventuels secrets persistants
duckdb -c ".read src/03_verify_duckdb.sql"
echo "== sorties : aws s3 ls processed/verification/"
aws s3 ls "s3://${BUCKET}/${PROCESSED_PREFIX}/verification/" --human-readable
