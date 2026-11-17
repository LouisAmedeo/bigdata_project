#!/usr/bin/env bash
# Étape 5 (facultative) : banc d'essai dans un Job Kubernetes dédié (~10 min).
#   bash src/04_benchmark.sh
# Pourquoi un Job : lancé dans le service, Spark (JVM) + VS Code dépassent la limite de 2000Mi
# du pod et le service est tué (OOMKilled, constaté le 2026-11-17).
# Le Job exécute src/04_benchmark_inpod.sh : pandas / DuckDB / Spark, CSV / Parquet, x1 / x10 / x100,
# puis le test des petits fichiers sur S3. Sorties : processed/benchmark/{mesures,petits_fichiers}.csv
# Relance : idempotente.

set -euo pipefail
cd "$(dirname "$0")/.."
. src/common.sh

banner "étape 5 : banc d'essai (Job k8s)"
check_s3
kubectl create configmap benchmark-config \
  --from-literal=BUCKET="$BUCKET" --from-literal=PROJECT_PREFIX="$PROJECT_PREFIX" \
  --from-literal=RAW_PREFIX="$RAW_PREFIX" --from-literal=RAW_FILE="$RAW_FILE" \
  --from-literal=PROCESSED_PREFIX="$PROCESSED_PREFIX" \
  --from-literal=AWS_ENDPOINT_URL="$S3_ENDPOINT_URL" --from-literal=AWS_S3_ENDPOINT="$S3_HOST" \
  --from-literal=AWS_DEFAULT_REGION="$S3_REGION" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap benchmark-code --from-file=src/04_benchmark.py \
  --from-file=src/04_benchmark_inpod.sh --from-file=src/02_transform_spark.py \
  --dry-run=client -o yaml | kubectl apply -f -
s3_secret
kubectl delete job benchmark-tgv --ignore-not-found
kubectl apply -f k8s/03-job-benchmark.yaml
wait_job benchmark-tgv 2400
echo "== logs du Job benchmark-tgv"
kubectl logs job/benchmark-tgv
kubectl get job,pods -l step=benchmark -o wide
aws s3 ls "s3://${BUCKET}/${PROCESSED_PREFIX}/benchmark/" --human-readable
