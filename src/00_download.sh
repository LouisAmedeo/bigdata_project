#!/usr/bin/env bash
# Étape 1 : téléchargement du dataset brut "Régularité mensuelle TGV par liaisons"
# Source : SNCF Open Data (AQST), licence ODbL
#
# Usage : bash src/00_download.sh [dossier_sortie]
# Le fichier est écrit dans data/raw/ (ignoré par git), avec un fichier .meta
# qui garde la date de téléchargement, la taille et le sha256 (traçabilité).

set -euo pipefail

DATASET_ID="regularite-mensuelle-tgv-aqst"
# Extraction figée : on s'arrête au mois de 2026-09 et on impose un ordre de tri,
# sinon le fichier change à chaque mise à jour mensuelle de la SNCF.
# -> les deux membres du binôme téléchargent exactement le même fichier.
LAST_MONTH="2026-09"
WHERE="date%3C%3D%22${LAST_MONTH}%22"                          # date <= "2026-09"
ORDER="date%2Cservice%2Cgare_depart%2Cgare_arrivee"
URL="https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/${DATASET_ID}/exports/csv?delimiter=%3B&where=${WHERE}&order_by=${ORDER}"
EXPECTED_SHA256="8b2f5a27593f32516f0781ab591d8316f9b09e639fa5f4d76496046e837602ee"

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

if [ "$SHA" = "$EXPECTED_SHA256" ]; then
  SHA_STATUS="ok"
else
  # Pas bloquant : la SNCF peut corriger d'anciens mois. On prévient seulement.
  SHA_STATUS="different"
  echo "ATTENTION : sha256 différent de la version de référence (${EXPECTED_SHA256})" >&2
fi

cat > "${OUT_FILE}.meta" <<EOF
dataset=${DATASET_ID}
url=${URL}
licence=ODbL
download_date=${DOWNLOAD_DATE}
size_bytes=${SIZE}
sha256=${SHA}
sha256_reference=${SHA_STATUS}
last_month=${LAST_MONTH}
nb_columns=${NB_COLS}
EOF

echo "OK : ${OUT_FILE}"
cat "${OUT_FILE}.meta"
