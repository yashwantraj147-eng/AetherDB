$jarFile = Join-Path $PSScriptRoot "target\aetherdb-2.0.2.jar"
if (-not (Test-Path $jarFile)) {
    Write-Host "[INFO] Packaging AetherDB executable jar..." -ForegroundColor Cyan
    & "$PSScriptRoot\build.ps1"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

Write-Host "Starting AetherDB Server..." -ForegroundColor Green
$configPath = Join-Path $PSScriptRoot "config\aether-cluster.properties"
& java -jar $jarFile $configPath
