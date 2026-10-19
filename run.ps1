Write-Host "Starting AetherDB Server..." -ForegroundColor Cyan

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

if (-not (Test-Path "$scriptDir/bin/com/aetherdb/AetherServer.class")) {
    Write-Host "AetherDB is not built yet. Compiling..." -ForegroundColor Yellow
    & "$scriptDir/build.ps1"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

& java -cp "$scriptDir/bin" com.aetherdb.AetherServer "$scriptDir/config/aether-cluster.properties"
