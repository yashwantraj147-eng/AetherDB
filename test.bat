@echo off
setlocal
echo ========================================================
echo   AetherDB Verification and Regression Test Suite
echo ========================================================

where mvn >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    mvn clean test
    exit /b %ERRORLEVEL%
)

if exist "%USERPROFILE%\apache-maven\apache-maven-3.9.6\bin\mvn.cmd" (
    call "%USERPROFILE%\apache-maven\apache-maven-3.9.6\bin\mvn.cmd" clean test
    exit /b %ERRORLEVEL%
)

echo [ERROR] Maven not found! Please install Apache Maven 3.9+ or add it to PATH.
exit /b 1
