@echo off
setlocal

set JAR_FILE=%~dp0target\aetherdb-2.0.0.jar
if not exist "%JAR_FILE%" (
    echo [INFO] Packaging AetherDB executable jar...
    call "%~dp0build.bat"
    if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%
)

echo Starting AetherDB Server...
java -jar "%JAR_FILE%" "%~dp0config\aether-cluster.properties"
