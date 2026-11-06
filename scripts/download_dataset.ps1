# Télécharge le dataset UCI "Diabetes 130-US Hospitals (1999-2008)" dans ./data
# Usage (PowerShell, depuis la racine du projet) : .\scripts\download_dataset.ps1
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Data = Join-Path $Root "data"
$Url  = "https://archive.ics.uci.edu/static/public/296/diabetes+130-us+hospitals+for+years+1999-2008.zip"
New-Item -ItemType Directory -Force $Data | Out-Null
if (Test-Path "$Data\diabetic_data.csv") { Write-Host "Dataset deja present : $Data\diabetic_data.csv"; exit 0 }
$Zip = Join-Path $env:TEMP "uci_diabetes.zip"
Write-Host "Telechargement depuis UCI..."
Invoke-WebRequest -Uri $Url -OutFile $Zip -UseBasicParsing
$Tmp = Join-Path $env:TEMP "uci_diabetes"
if (Test-Path $Tmp) { Remove-Item -Recurse -Force $Tmp }
Expand-Archive -Path $Zip -DestinationPath $Tmp
# L'archive peut contenir un sous-dossier ou une seconde archive : on cherche partout
Get-ChildItem $Tmp -Recurse -Filter *.zip | ForEach-Object { Expand-Archive $_.FullName -DestinationPath $Tmp -Force }
foreach ($f in @("diabetic_data.csv", "IDs_mapping.csv")) {
  $src = Get-ChildItem $Tmp -Recurse -Filter $f | Select-Object -First 1
  if ($src) { Copy-Item $src.FullName (Join-Path $Data $f) -Force }
}
$lines = (Get-Content "$Data\diabetic_data.csv" | Measure-Object -Line).Lines
Write-Host "OK : $Data\diabetic_data.csv ($($lines - 1) sejours)"
