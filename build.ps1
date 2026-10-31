Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  AetherDB Build Script - Maven Java 21 Canonical Build" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan

$mvnCmd = Get-Command mvn -ErrorAction SilentlyContinue
if ($null -ne $mvnCmd) {
    & mvn clean package -DskipTests
    exit $LASTEXITCODE
}

$localMvn = "$env:USERPROFILE\apache-maven\apache-maven-3.9.6\bin\mvn.cmd"
if (Test-Path $localMvn) {
    & $localMvn clean package -DskipTests
    exit $LASTEXITCODE
}

Write-Error "Maven executable (mvn) not found! Please install Apache Maven 3.9+ or add it to PATH."
exit 1
