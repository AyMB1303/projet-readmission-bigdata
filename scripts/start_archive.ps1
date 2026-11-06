# Lance le job Spark Structured Streaming Kafka -> HDFS sur le cluster (en arriere-plan)
# puis affiche le debut de son journal pour verifier qu'il a bien demarre.
# Usage : powershell -ExecutionPolicy Bypass -File scripts\start_archive.ps1
$running = docker exec spark-master bash -c "grep -l 'archive_to_hdf[s].py' /proc/[0-9]*/cmdline 2>/dev/null | head -n 1"
if ($running) { Write-Host "Le job d'archivage tourne deja ($running)." -ForegroundColor Yellow; exit 0 }
docker exec -d spark-master bash -c "/opt/spark/bin/spark-submit --name archive-kafka-to-hdfs /opt/app/streaming/archive_to_hdfs.py > /tmp/archive_raw.log 2>&1"
Write-Host "Job soumis. Attente du demarrage (telechargement du connecteur Kafka au 1er lancement)..."
for ($i = 0; $i -lt 24; $i++) {
  Start-Sleep 5
  $log = docker exec spark-master bash -c "cat /tmp/archive_raw.log 2>/dev/null"
  if ("$log" -match "Archivage d.marr|batch \d+|Exception|Error|command not found") { break }
}
Write-Host "--- Journal du job (dernieres lignes) ---" -ForegroundColor Cyan
docker exec spark-master bash -c "grep -vE '^\s*$|WARN NativeCodeLoader|resolving|downloading|found org|:: ' /tmp/archive_raw.log | tail -n 15"
Write-Host "Suivi : http://localhost:8080 (applications) et http://localhost:4040 (Structured Streaming)"
