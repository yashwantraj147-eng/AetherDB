Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  AetherDB Build Script - Java 26 Native Compiler" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan

if (-not (Test-Path "bin")) {
    New-Item -ItemType Directory -Path "bin" | Out-Null
}

$mainSources = Get-ChildItem -Recurse -Filter *.java -Path "src/main/java" | ForEach-Object { $_.FullName }
$testSources = @()
if (Test-Path "src/test/java") {
    $testSources = Get-ChildItem -Recurse -Filter *.java -Path "src/test/java" | ForEach-Object { $_.FullName }
}

$allSources = @($mainSources) + @($testSources)

if ($null -eq $allSources -or $allSources.Count -eq 0) {
    Write-Error "No Java source files found!"
    exit 1
}

Write-Host "Compiling $($allSources.Count) AetherDB source files with javac 26..." -ForegroundColor Yellow
& javac -d bin --release 26 $allSources

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
