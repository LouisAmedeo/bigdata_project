#!/usr/bin/env bash
# Étape 3 : transformation Spark (nettoyage, qualité, agrégations) dans un Job Kubernetes.
# À lancer depuis le terminal du service vscode-pyspark, à la racine du dépôt :
#   bash src/02_transform.sh
# Entrée : s3://$BUCKET/bigdata_project/raw/...   Sorties : s3://$BUCKET/bigdata_project/processed/
# Relance : idempotente (ConfigMaps/Secret en apply, ancien Job supprimé, écritures Spark en overwrite).
# Variante sans Job (même script, driver dans le service) :
#   RAW_PATH=s3a://$BUCKET/... spark-submit --master 'local[2]' src/02_transform_spark.py

set -euo pipefail
cd "$(dirname "$0")/.."
. src/common.sh

banner "étape 3 : transformation Spark (Job k8s)"
check_s3
if ! aws s3 ls "s3://${BUCKET}/${RAW_PREFIX}/${RAW_FILE}" >/dev/null 2>&1; then
  echo "ERREUR : données brutes absentes, lancer d'abord src/01_ingest_raw.sh" >&2; exit 1
fi

# 1. Configuration + code du traitement (le script Python est monté dans le pod via une ConfigMap)
kubectl create configmap transform-config \
  --from-literal=BUCKET="$BUCKET" \
  --from-literal=RAW_PATH="s3a://${BUCKET}/${RAW_PREFIX}/${RAW_FILE}" \
  --from-literal=PROCESSED_PATH="s3a://${BUCKET}/${PROCESSED_PREFIX}" \
  --from-literal=AWS_ENDPOINT_URL="$S3_ENDPOINT_URL" \
  --from-literal=AWS_S3_ENDPOINT="$S3_HOST" \
  --from-literal=AWS_DEFAULT_REGION="$S3_REGION" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap spark-transform-code --from-file=src/02_transform_spark.py \
  --dry-run=client -o yaml | kubectl apply -f -
s3_secret

# 2. Job (un Job terminé ne se relance pas : on supprime puis on recrée)
kubectl delete job transform-spark-tgv --ignore-not-found
kubectl apply -f k8s/02-job-spark-transform.yaml
wait_job transform-spark-tgv 1200

# 3. Traces d'exécution (logs Spark sans le bruit INFO/WARN du démarrage)
echo "== logs du Job transform-spark-tgv"
kubectl logs job/transform-spark-tgv | grep -vE '^[0-9]{2}/[0-9]{2}/[0-9]{2} [0-9:]{8} (INFO|WARN)|^\s*$|NativeCodeLoader'
kubectl get job,pods -l step=transform-spark -o wide

# 4. Vérification depuis le terminal : ce qui a été écrit dans processed/
echo "== vérification : aws s3 ls processed/"
aws s3 ls "s3://${BUCKET}/${PROCESSED_PREFIX}/" --recursive --human-readable --summarize | grep -v '_SUCCESS'
