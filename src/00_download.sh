#!/usr/bin/env bash
# Étape 1 : téléchargement du dataset brut "Régularité mensuelle TGV par liaisons"
# Source : SNCF Open Data (AQST), licence ODbL
#
# Usage : bash src/00_download.sh [dossier_sortie]
# Le fichier est écrit dans data/raw/ (ignoré par git), avec un fichier .meta
# qui garde la date de téléchargement, la taille et le sha256 (traçabilité).

set -euo pipefail

DATASET_ID="regularite-mensuelle-tgv-aqst"
URL="https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/${DATASET_ID}/exports/csv?delimiter=%3B"

OUT_DIR="${1:-data/raw}"
DOWNLOAD_DATE="$(date +%Y-%m-%d)"
OUT_FILE="${OUT_DIR}/${DATASET_ID}_${DOWNLOAD_DATE}.csv"

# Contrainte du projet : données brutes entre 1 Mo et 5 Mo
MIN_BYTES=1000000
MAX_BYTES=5000000

mkdir -p "$OUT_DIR"

echo "Téléchargement de ${DATASET_ID} ..."
curl --fail --silent --show-error --location --retry 3 -o "$OUT_FILE" "$URL"

SIZE=$(wc -c < "$OUT_FILE" | tr -d ' ')
if command -v sha256sum >/dev/null 2>&1; then
  SHA=$(sha256sum "$OUT_FILE" | cut -d' ' -f1)
else
  SHA=$(shasum -a 256 "$OUT_FILE" | cut -d' ' -f1)   # macOS
fi
HEADER=$(head -1 "$OUT_FILE" | sed 's/^\xEF\xBB\xBF//')   # retire le BOM UTF-8
NB_COLS=$(echo "$HEADER" | awk -F';' '{print NF}')

if [ "$SIZE" -lt "$MIN_BYTES" ] || [ "$SIZE" -gt "$MAX_BYTES" ]; then
  echo "ERREUR : taille ${SIZE} octets hors de l'intervalle [1 Mo ; 5 Mo]" >&2
  exit 1
fi

cat > "${OUT_FILE}.meta" <<EOF
dataset=${DATASET_ID}
url=${URL}
licence=ODbL
download_date=${DOWNLOAD_DATE}
size_bytes=${SIZE}
sha256=${SHA}
nb_columns=${NB_COLS}
EOF

echo "OK : ${OUT_FILE}"
cat "${OUT_FILE}.meta"
