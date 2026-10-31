@echo off
setlocal
echo ========================================================
echo   AetherDB Build Script - Maven Java 21 Canonical Build
echo ========================================================

where mvn >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    mvn clean package -DskipTests
    exit /b %ERRORLEVEL%
)

echo [INFO] Maven not in global PATH, checking local toolchain...
if exist "%USERPROFILE%\apache-maven\apache-maven-3.9.6\bin\mvn.cmd" (
    call "%USERPROFILE%\apache-maven\apache-maven-3.9.6\bin\mvn.cmd" clean package -DskipTests
    exit /b %ERRORLEVEL%
)

echo [ERROR] Maven executable (mvn) not found! Please install Apache Maven 3.9+ or add it to PATH.
exit /b 1
