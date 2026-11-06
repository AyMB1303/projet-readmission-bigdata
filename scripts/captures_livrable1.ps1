# Capture automatiquement les interfaces web du cluster (preuves du livrable 1)
# Usage : powershell -ExecutionPolicy Bypass -File scripts\captures_livrable1.ps1
# Les images brutes sont enregistrees dans docs\captures\raw\
$Root = Split-Path -Parent $PSScriptRoot
$Out = Join-Path $Root "docs\captures\raw"
New-Item -ItemType Directory -Force $Out | Out-Null
Add-Type @"
using System; using System.Runtime.InteropServices;
public class W {
  [DllImport("user32.dll")] public static extern IntPtr GetForegroundWindow();
  [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr h);
  [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr h, int c);
  [DllImport("user32.dll")] public static extern void keybd_event(byte b, byte s, uint f, UIntPtr e);
  [DllImport("user32.dll")] public static extern bool SetProcessDPIAware();
  [DllImport("dwmapi.dll")] public static extern int DwmGetWindowAttribute(IntPtr h, int a, out RECT r, int s);
  public struct RECT { public int L, T, R, B; }
}
"@
Add-Type -AssemblyName System.Drawing
[W]::SetProcessDPIAware() | Out-Null
$Term = [W]::GetForegroundWindow()

function Browser() {
  foreach ($n in @("chrome", "msedge", "firefox", "brave")) {
    $p = Get-Process $n -ErrorAction SilentlyContinue | Where-Object { $_.MainWindowHandle -ne 0 } | Select-Object -First 1
    if ($p) { return $p.MainWindowHandle }
  }
  return [W]::GetForegroundWindow()
}
function Focus($h) {
  [W]::ShowWindow($h, 3) | Out-Null
  [W]::keybd_event(0x12, 0, 0, [UIntPtr]::Zero); [W]::keybd_event(0x12, 0, 2, [UIntPtr]::Zero)
  [W]::SetForegroundWindow($h) | Out-Null; Start-Sleep -Milliseconds 800
}
function Grab($name, $h) {
  $r = New-Object W+RECT
  [W]::DwmGetWindowAttribute($h, 9, [ref]$r, 16) | Out-Null
  $bmp = New-Object System.Drawing.Bitmap ($r.R - $r.L), ($r.B - $r.T)
  $g = [System.Drawing.Graphics]::FromImage($bmp)
  $g.CopyFromScreen($r.L, $r.T, 0, 0, $bmp.Size)
  $bmp.Save((Join-Path $Out "$name.png"), [System.Drawing.Imaging.ImageFormat]::Png)
  $g.Dispose(); $bmp.Dispose()
  Write-Host "  capture : $name.png"
}

# Le job d'archivage doit tourner pour l'interface Structured Streaming (port 4040)
try { Invoke-WebRequest -UseBasicParsing -TimeoutSec 5 http://localhost:4040 | Out-Null }
catch { Write-Host "Job d'archivage non actif : lancement..."; & "$PSScriptRoot\start_archive.ps1"; Start-Sleep 30 }

$pages = [ordered]@{
  "01_hdfs_overview"      = "http://localhost:9870/dfshealth.html#tab-overview"
  "02_hdfs_datanodes"     = "http://localhost:9870/dfshealth.html#tab-datanode"
  "03_hdfs_archive"       = "http://localhost:9870/explorer.html#/data/raw/hospital_discharges"
  "04_spark_master"       = "http://localhost:8080/"
  "05_spark_streaming"    = "http://localhost:4040/StreamingQuery/"
  "06_spark_jobs"         = "http://localhost:4040/jobs/"
  "07_kafka_topics"       = "http://localhost:8090/ui/clusters/readmission/all-topics"
  "08_kafka_messages"     = "http://localhost:8090/ui/clusters/readmission/all-topics/hospital-discharges/messages"
}
Write-Host "Ne touchez ni a la souris ni au clavier pendant environ 1 minute 30..." -ForegroundColor Yellow
foreach ($k in $pages.Keys) {
  Start-Process $pages[$k]; Start-Sleep 9
  $b = Browser; Focus $b; Grab $k $b
}
Focus $Term
Write-Host "Captures enregistrees dans $Out" -ForegroundColor Green
