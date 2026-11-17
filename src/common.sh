#!/usr/bin/env bash
# Paramètres et fonctions communs aux étapes du pipeline (sourcé par src/01..04_*.sh).
# À lancer depuis le terminal d'un service Onyxia (vscode-pyspark) : les identifiants S3
# temporaires y sont fournis par les variables AWS_* ; à défaut on lit le profil aws "default".

BUCKET="${BUCKET:-${KUBERNETES_NAMESPACE:?KUBERNETES_NAMESPACE non défini, lancer depuis Onyxia ou définir BUCKET}}"
PROJECT_PREFIX="bigdata_project"
EXTRACTION="2026-09"
RAW_PREFIX="${PROJECT_PREFIX}/raw/regularite_tgv/extraction=${EXTRACTION}"
RAW_FILE="regularite-mensuelle-tgv-aqst.csv"
PROCESSED_PREFIX="${PROJECT_PREFIX}/processed"

S3_ENDPOINT_URL="${AWS_ENDPOINT_URL:-$(aws configure get endpoint_url --profile default 2>/dev/null || true)}"
S3_ENDPOINT_URL="${S3_ENDPOINT_URL%/}"
if [ -z "$S3_ENDPOINT_URL" ]; then echo "ERREUR : endpoint S3 introuvable (AWS_ENDPOINT_URL)" >&2; exit 1; fi
S3_HOST="${AWS_S3_ENDPOINT:-${S3_ENDPOINT_URL#*://}}"
S3_REGION="${AWS_DEFAULT_REGION:-$(aws configure get region --profile default 2>/dev/null || echo us-east-1)}"
export BUCKET AWS_S3_ENDPOINT="$S3_HOST"

banner() {   # en-tête de chaque étape : identifiant Onyxia + date (exigé pour les preuves)
  echo "== $1 | namespace : ${KUBERNETES_NAMESPACE:-?} | bucket : ${BUCKET} | $(date '+%F %T %Z')"
}

check_s3() {  # échoue tôt et clairement si les identifiants temporaires (12 h) ont expiré
  if ! aws s3 ls "s3://${BUCKET}/" >/dev/null 2>&1; then
    echo "ERREUR : accès refusé à s3://${BUCKET} (identifiants Onyxia expirés ?) -> voir README, Relance" >&2
    exit 1
  fi
}

s3_secret() {  # Secret k8s s3-credentials à partir des identifiants courants, sans jamais les afficher
  local tmp; tmp="$(mktemp)"; chmod 600 "$tmp"
  if [ -n "${AWS_ACCESS_KEY_ID:-}" ]; then
    printf 'AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n' "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" > "$tmp"
    if [ -n "${AWS_SESSION_TOKEN:-}" ]; then printf 'AWS_SESSION_TOKEN=%s\n' "$AWS_SESSION_TOKEN" >> "$tmp"; fi
  else
    aws configure export-credentials --profile default --format env-no-export \
      | grep -E '^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=' > "$tmp"
  fi
  kubectl create secret generic s3-credentials --from-env-file="$tmp" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  rm -f "$tmp"
  echo "secret/s3-credentials configured (valeurs non affichées)"
}

wait_job() {  # attend la fin d'un Job (succès ou échec) au lieu d'un simple timeout
  local job="$1" timeout="${2:-600}" t=0 ok ko
  while [ "$t" -lt "$timeout" ]; do
    ok="$(kubectl get job "$job" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || true)"
    ko="$(kubectl get job "$job" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || true)"
    if [ "$ok" = "True" ]; then echo "job.batch/$job complete (${t} s)"; return 0; fi
    if [ "$ko" = "True" ]; then
      echo "ERREUR : job $job en échec (backoffLimit atteint)" >&2; kubectl describe job "$job" | tail -15 >&2; return 1
    fi
    sleep 5; t=$((t + 5))
  done
  echo "ERREUR : job $job non terminé après ${timeout} s" >&2; return 1
}
