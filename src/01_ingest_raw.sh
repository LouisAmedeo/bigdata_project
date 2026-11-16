#!/usr/bin/env bash
# Étape 2 : dépôt des données brutes sur le S3 d'Onyxia via un Job Kubernetes.
# À lancer depuis le terminal du service vscode-pyspark, à la racine du dépôt :
#   bash src/01_ingest_raw.sh
#
# Le bucket est celui de la personne qui lance le script ($KUBERNETES_NAMESPACE),
# on peut le forcer avec BUCKET=... bash src/01_ingest_raw.sh
# Relance : le script est idempotent (il supprime l'ancien Job puis le recrée,
# et l'upload écrase le même objet).

set -euo pipefail

BUCKET="${BUCKET:-${KUBERNETES_NAMESPACE:?KUBERNETES_NAMESPACE non défini, lancer depuis Onyxia ou définir BUCKET}}"
RAW_PREFIX="bigdata_project/raw/regularite_tgv/extraction=2026-09"
RAW_FILE="regularite-mensuelle-tgv-aqst.csv"
SOURCE_URL="https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/regularite-mensuelle-tgv-aqst/exports/csv?delimiter=%3B&where=date%3C%3D%222026-09%22&order_by=date%2Cservice%2Cgare_depart%2Cgare_arrivee"
EXPECTED_SHA256="8b2f5a27593f32516f0781ab591d8316f9b09e639fa5f4d76496046e837602ee"

S3_ENDPOINT_URL="$(aws configure get endpoint_url --profile default)"
S3_REGION="$(aws configure get region --profile default)"

echo "== namespace : $(kubectl config view --minify -o jsonpath='{..namespace}')  | bucket : ${BUCKET}  | $(date '+%F %T %Z')"

# 1. Configuration non sensible -> ConfigMap (apply = idempotent)
kubectl create configmap ingest-config \
  --from-literal=SOURCE_URL="$SOURCE_URL" \
  --from-literal=EXPECTED_SHA256="$EXPECTED_SHA256" \
  --from-literal=STRICT_SHA256="true" \
  --from-literal=BUCKET="$BUCKET" \
  --from-literal=RAW_PREFIX="$RAW_PREFIX" \
  --from-literal=RAW_FILE="$RAW_FILE" \
  --from-literal=AWS_ENDPOINT_URL="$S3_ENDPOINT_URL" \
  --from-literal=AWS_DEFAULT_REGION="$S3_REGION" \
  --dry-run=client -o yaml | kubectl apply -f -

# 2. Identifiants -> Secret, via un fichier temporaire jamais affiché (comme au TP3)
TMP_ENV="$(mktemp)"; chmod 600 "$TMP_ENV"
trap 'rm -f "$TMP_ENV"' EXIT
aws configure export-credentials --profile default --format env-no-export \
  | grep -E '^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=' > "$TMP_ENV"
kubectl create secret generic s3-credentials --from-env-file="$TMP_ENV" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "secret/s3-credentials configured (valeurs non affichées)"

# 3. Job : on supprime l'ancien s'il existe (un Job terminé ne se relance pas)
kubectl delete job ingest-raw-tgv --ignore-not-found
kubectl apply -f k8s/01-job-ingest-raw.yaml
kubectl wait --for=condition=complete job/ingest-raw-tgv --timeout=300s

# 4. Traces d'exécution (preuves)
echo "== logs initContainer download"
kubectl logs job/ingest-raw-tgv -c download
echo "== logs container upload"
kubectl logs job/ingest-raw-tgv -c upload
kubectl get job,pods -l step=ingest-raw -o wide

# 5. Vérification depuis le terminal, indépendamment du Job
echo "== vérification : aws s3 ls"
aws s3 --profile default ls "s3://${BUCKET}/bigdata_project/raw/" --recursive --human-readable
