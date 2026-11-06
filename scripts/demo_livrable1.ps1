# Démonstration complète du livrable 1, de zéro jusqu'à la vérification.
# Usage : powershell -ExecutionPolicy Bypass -File scripts\demo_livrable1.ps1
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
function Step($t) { Write-Host "`n=== $t ===" -ForegroundColor Cyan }

Step "0. Configuration"
if (-not (Test-Path .env)) { Copy-Item .env.example .env; Write-Host ".env cree depuis .env.example" }

Step "1. Dataset UCI"
& "$PSScriptRoot\download_dataset.ps1"
if (-not (Test-Path data\diabetic_data.csv)) { Write-Host "Dataset absent : arret." -ForegroundColor Red; exit 1 }

Step "2. Demarrage du cluster (build + up)"
docker compose build spark-master
docker compose up -d
if ($LASTEXITCODE -ne 0) { Write-Host "Echec du demarrage du cluster : voir les erreurs ci-dessus." -ForegroundColor Red; exit 1 }
Write-Host "Attente de HDFS (sortie du safe mode) et des workers Spark..."
for ($i = 0; $i -lt 40; $i++) {
  $sm = docker exec namenode /opt/hadoop/bin/hdfs dfsadmin -safemode get 2>$null
  $dn = docker exec namenode bash -c "/opt/hadoop/bin/hdfs dfsadmin -report 2>/dev/null | grep -o 'Live datanodes ([0-9]*)'"
  if ("$sm" -match "OFF" -and "$dn" -match "\(2\)") { break }
  Start-Sleep 5
}
Write-Host "HDFS : $sm | $dn"
docker compose ps

Step "3. Job Spark d'archivage Kafka -> HDFS"
& "$PSScriptRoot\start_archive.ps1"
Start-Sleep 40

Step "4. Producteur : rejeu du dataset (200 evt/s pour la demo)"
$env:EVENTS_PER_SECOND = "200"
docker compose --profile ingestion up -d --force-recreate producer
Start-Sleep 90
docker compose logs --tail 5 producer

Step "5. Verification"
& "$PSScriptRoot\verify_livrable1.ps1"
Write-Host "`nInterfaces : HDFS http://localhost:9870 | Spark http://localhost:8080 | Kafka UI http://localhost:8090" -ForegroundColor Green
