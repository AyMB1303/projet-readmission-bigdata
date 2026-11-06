#!/usr/bin/env bash
# Télécharge le dataset UCI dans ./data (Linux / WSL / macOS)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; DATA="$ROOT/data"; mkdir -p "$DATA"
URL="https://archive.ics.uci.edu/static/public/296/diabetes+130-us+hospitals+for+years+1999-2008.zip"
[ -f "$DATA/diabetic_data.csv" ] && { echo "Dataset deja present"; exit 0; }
TMP="$(mktemp -d)"; curl -fL "$URL" -o "$TMP/uci.zip"; unzip -q "$TMP/uci.zip" -d "$TMP"
find "$TMP" -name '*.zip' ! -name uci.zip -exec unzip -q -o {} -d "$TMP" \;
for f in diabetic_data.csv IDs_mapping.csv; do find "$TMP" -name "$f" -exec cp {} "$DATA/" \; ; done
echo "OK : $(($(wc -l < "$DATA/diabetic_data.csv") - 1)) sejours dans $DATA/diabetic_data.csv"
