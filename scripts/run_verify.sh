#!/usr/bin/env bash
# Vérifie l'output du Livrable 1 : intégrité, schéma, sentinelles, partitionnement, relecture.
# Redirige le rapport Markdown vers docs/verification_output_livrable1.md.
set -euo pipefail
cd "$(dirname "$0")/.."

REPORT="docs/verification_output_livrable1.md"

echo "Lancement de la vérification sur spark-master..."
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --conf "spark.app.name=verify-livrable1" \
    /opt/app/data/verify_output.py \
    2>/dev/null | tee "$REPORT"

echo ""
echo "Rapport sauvegardé dans $REPORT"
