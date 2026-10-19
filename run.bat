@echo off
setlocal

if not exist "bin\com\aetherdb\AetherServer.class" (
    echo AetherDB is not built yet. Running build.bat...
    call "%~dp0build.bat"
    if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%
)

echo Starting AetherDB Server...
java -cp "%~dp0bin" com.aetherdb.AetherServer "%~dp0config\aether-cluster.properties"
