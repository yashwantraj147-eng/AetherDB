@echo off
setlocal

set JAR_FILE=%~dp0target\aetherdb-2.0.1.jar
if exist "%JAR_FILE%" goto :start_server

echo [INFO] Packaging AetherDB executable jar...
call "%~dp0build.bat"
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Build failed with exit code %ERRORLEVEL%
    exit /b %ERRORLEVEL%
)

:start_server
echo Starting AetherDB Server...
java -jar "%JAR_FILE%" "%~dp0config\aether-cluster.properties"
exit /b %ERRORLEVEL%
