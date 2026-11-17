#!/usr/bin/env bash
# Étape 2 : dépôt des données brutes sur le S3 d'Onyxia via un Job Kubernetes.
# À lancer depuis le terminal du service vscode-pyspark, à la racine du dépôt :
#   bash src/01_ingest_raw.sh
#
# Le bucket est celui de la personne qui lance le script ($KUBERNETES_NAMESPACE),
# on peut le forcer avec BUCKET=... bash src/01_ingest_raw.sh
# Relance : le script est idempotent (il supprime l'ancien Job puis le recrée,
# et l'upload écrase le même objet).
# Identifiants : variables AWS_* du service Onyxia (ou profil aws "default"), voir src/common.sh.

set -euo pipefail
cd "$(dirname "$0")/.."
. src/common.sh          # BUCKET, RAW_PREFIX, RAW_FILE, endpoint S3, s3_secret, wait_job

SOURCE_URL="https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/regularite-mensuelle-tgv-aqst/exports/csv?delimiter=%3B&where=date%3C%3D%222026-09%22&order_by=date%2Cservice%2Cgare_depart%2Cgare_arrivee"
EXPECTED_SHA256="8b2f5a27593f32516f0781ab591d8316f9b09e639fa5f4d76496046e837602ee"

banner "étape 2 : ingestion raw (Job k8s)"
check_s3

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

# 2. Identifiants -> Secret (valeurs jamais affichées, cf. s3_secret dans common.sh)
s3_secret

# 3. Job : on supprime l'ancien s'il existe (un Job terminé ne se relance pas)
kubectl delete job ingest-raw-tgv --ignore-not-found
kubectl apply -f k8s/01-job-ingest-raw.yaml
wait_job ingest-raw-tgv 300

# 4. Traces d'exécution (preuves)
echo "== logs initContainer download"
kubectl logs job/ingest-raw-tgv -c download
echo "== logs container upload"
kubectl logs job/ingest-raw-tgv -c upload
kubectl get job,pods -l step=ingest-raw -o wide

# 5. Vérification depuis le terminal, indépendamment du Job
echo "== vérification : aws s3 ls"
aws s3 ls "s3://${BUCKET}/${PROJECT_PREFIX}/raw/" --recursive --human-readable
