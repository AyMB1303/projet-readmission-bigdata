# Vérification du livrable 1 : cluster, Kafka, ingestion, archivage HDFS
# Usage : .\scripts\verify_livrable1.ps1   -> écrit aussi docs\verification_livrable1.txt
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$Out = Join-Path $Root "docs\verification_livrable1.txt"
"Verification livrable 1 - $(Get-Date -Format 'yyyy-MM-dd HH:mm')" | Set-Content $Out -Encoding UTF8
function Section($title, [scriptblock]$cmd) {
  $h = "`n===== $title ====="
  Write-Host $h -ForegroundColor Cyan; Add-Content $Out $h
  $r = & $cmd 2>&1 | ForEach-Object { "$_" }
  $r | ForEach-Object { Write-Host $_ }; $r | Add-Content $Out
}
Section "1. Conteneurs" { docker compose ps --format "table {{.Name}}\t{{.Image}}\t{{.Status}}" }
Section "2. HDFS : DataNodes vivants et replication" {
  docker exec namenode bash -c "/opt/hadoop/bin/hdfs dfsadmin -report | grep -E 'Live datanodes|^Name:|Hostname|DFS Remaining%'"
  docker exec namenode /opt/hadoop/bin/hdfs getconf -confKey dfs.replication
}
Section "3. Spark : workers enregistres aupres du master" {
  $j = (docker exec spark-master curl -s http://localhost:8080/json/) -join "" | ConvertFrom-Json
  "status: $($j.status) | workers vivants: $($j.aliveworkers) | cores: $($j.cores) | memoire: $($j.memory) MB"
  foreach ($w in $j.workers) { " - $($w.host) : $($w.state), $($w.cores) cores, $($w.memory) MB" }
  "applications actives: " + (($j.activeapps | ForEach-Object { $_.name }) -join ", ")
}
Section "4. Kafka : topics" {
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic hospital-discharges
}
Section "5. Kafka : offsets (nombre de messages par partition)" {
  docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic hospital-discharges
}
Section "6. Kafka : 2 messages lus depuis le debut" {
  docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hospital-discharges --from-beginning --max-messages 2 --property print.key=true --timeout-ms 15000
}
Section "7. HDFS : archive Parquet ecrite par Spark" {
  docker exec namenode bash -c "/opt/hadoop/bin/hdfs dfs -ls -R /data/raw/hospital_discharges | head -n 12"
  docker exec namenode bash -c "echo -n 'Nombre de fichiers Parquet : '; /opt/hadoop/bin/hdfs dfs -ls -R /data/raw/hospital_discharges | grep -c '.parquet$'"
  docker exec namenode /opt/hadoop/bin/hdfs dfs -du -s -h /data/raw/hospital_discharges
}
Section "8. HDFS : replication effective des fichiers (fsck)" {
  docker exec namenode bash -c "/opt/hadoop/bin/hdfs fsck /data/raw -files -blocks | grep -E 'repl=|Average block replication|Status' | tail -n 6"
}
Section "9. Journal du job d'archivage (fin)" {
  docker exec spark-master bash -c "grep -vE '^\s*$|resolving|downloading|found org|:: ' /tmp/archive_raw.log 2>&1 | tail -n 15"
}
Write-Host "`nRapport ecrit dans $Out" -ForegroundColor Green
