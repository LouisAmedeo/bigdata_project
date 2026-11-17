#!/usr/bin/env bash
# Étape 5 (facultative) : mesures qui justifient les choix techniques du rapport.
# Exécuté DANS le pod du Job k8s/03-job-benchmark.yaml (lancé par src/04_benchmark.sh) :
# 2 CPU / 2000Mi, comme le service vscode-pyspark, mais sans VS Code à côté.
# A. pandas vs DuckDB vs Spark local[2], CSV vs Parquet, à x1 / x10 / x100 (src/04_benchmark.py)
# B. petits fichiers sur S3 : silver écrit avec 200 partitions de shuffle sans repartition
#    (réglage par défaut de Spark) contre le réglage retenu (8 partitions + repartition par année)
# Sorties : s3://$BUCKET/bigdata_project/processed/benchmark/{mesures,petits_fichiers}.csv
# Relance : idempotente (dossier de travail recréé, préfixe temporaire S3 effacé à la fin).

set -euo pipefail
cd /app
: "${BUCKET:?}" "${RAW_PREFIX:?}" "${RAW_FILE:?}" "${PROCESSED_PREFIX:?}" "${PROJECT_PREFIX:?}"
echo "== banc d'essai | bucket : ${BUCKET} | pod : $(hostname) | $(date '+%F %T %Z') | CPU limite : $(cat /sys/fs/cgroup/cpu.max) | mémoire limite : $(( $(cat /sys/fs/cgroup/memory.max) / 1048576 )) Mi"
WORK="${BENCH_DIR:-/tmp/bench_tgv}"
TMP_PREFIX="${PROJECT_PREFIX}/tmp_benchmark"
rm -rf "$WORK"; mkdir -p "$WORK"
aws s3 cp --only-show-errors "s3://${BUCKET}/${RAW_PREFIX}/${RAW_FILE}" "$WORK/brut.csv"
aws s3 sync --only-show-errors --exclude "*_SUCCESS" \
  "s3://${BUCKET}/${PROCESSED_PREFIX}/silver/regularite_mensuelle/" "$WORK/silver/"

echo "== A. moteurs x formats x échelles (3 passages, médiane)"
python /app/04_benchmark.py "$WORK" "$WORK/brut.csv" "$WORK/silver" "$WORK/mesures.csv"

echo "== B. petits fichiers : écriture du silver sur S3"
echo "shuffle_partitions,repartition_annee,fichiers_parquet_silver,taille_silver_octets,ecriture_silver_s,duree_totale_s" > "$WORK/petits_fichiers.csv"
for conf in "200 0" "8 1"; do
  set -- $conf
  OUTP="s3a://${BUCKET}/${TMP_PREFIX}/shuffle$1"
  SHUFFLE_PARTITIONS=$1 SILVER_REPARTITION=$2 RAW_PATH="s3a://${BUCKET}/${RAW_PREFIX}/${RAW_FILE}" PROCESSED_PATH="$OUTP" \
    /usr/local/lib/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g --conf spark.ui.enabled=false \
    /app/02_transform_spark.py > "$WORK/transform_shuffle$1.log" 2>&1
  LISTE="$(aws s3 ls --recursive "s3://${BUCKET}/${TMP_PREFIX}/shuffle$1/silver/" | grep '\.parquet$')"
  NB="$(echo "$LISTE" | wc -l)"; OCT="$(echo "$LISTE" | awk '{s+=$3} END {print s}')"
  ECR="$(grep -o 'écriture silver : [0-9.]*' "$WORK/transform_shuffle$1.log" | grep -o '[0-9.]*$')"
  TOT="$(grep -o 'terminé en [0-9.]*' "$WORK/transform_shuffle$1.log" | grep -o '[0-9.]*$')"
  echo "$1,$2,$NB,$OCT,$ECR,$TOT" | tee -a "$WORK/petits_fichiers.csv"
done
aws s3 rm --recursive --only-show-errors "s3://${BUCKET}/${TMP_PREFIX}/"

aws s3 cp --only-show-errors "$WORK/mesures.csv" "s3://${BUCKET}/${PROCESSED_PREFIX}/benchmark/mesures.csv"
aws s3 cp --only-show-errors "$WORK/petits_fichiers.csv" "s3://${BUCKET}/${PROCESSED_PREFIX}/benchmark/petits_fichiers.csv"
echo "== résultats"
tableau() { if command -v column >/dev/null; then column -t -s,; else cat; fi; }
cut -d, -f1-9 "$WORK/mesures.csv" | tableau
tableau < "$WORK/petits_fichiers.csv"
rm -rf "$WORK"
