Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  AetherDB Build Script - Java 26 Native Compiler" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan

if (-not (Test-Path "bin")) {
    New-Item -ItemType Directory -Path "bin" | Out-Null
}

$sources = Get-ChildItem -Recurse -Filter *.java -Path "src/main/java" | ForEach-Object { $_.FullName }
if ($null -eq $sources -or $sources.Count -eq 0) {
    Write-Error "No Java source files found!"
    exit 1
}

Write-Host "Compiling $($sources.Count) AetherDB source files with javac 26..." -ForegroundColor Yellow
& javac -d bin --release 26 $sources

if ($LASTEXITCODE -ne 0) {
    Write-Host "[BUILD FAILED] Compilation error." -ForegroundColor Red
    exit $LASTEXITCODE
}

if (Test-Path "web") {
    Copy-Item -Recurse -Force "web" "bin/"
}
if (Test-Path "config") {
    Copy-Item -Recurse -Force "config" "bin/"
}

Write-Host "[BUILD SUCCESSFUL] AetherDB compiled cleanly into bin/" -ForegroundColor Green
