# Vérifie l'output du Livrable 1 — Windows PowerShell
# Redirige le rapport Markdown vers docs/verification_output_livrable1.md.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root   = Split-Path $PSScriptRoot -Parent
$Report = Join-Path $Root "docs\verification_output_livrable1.md"

Write-Host "Lancement de la vérification sur spark-master..."
$output = docker exec spark-master spark-submit `
    --master spark://spark-master:7077 `
    --conf "spark.app.name=verify-livrable1" `
    /opt/app/data/verify_output.py 2>$null

$output | Tee-Object -FilePath $Report
Write-Host ""
Write-Host "Rapport sauvegardé dans $Report"
