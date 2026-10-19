@echo off
setlocal

if not exist "bin\com\aetherdb\test\TestRunner.class" (
    echo Building AetherDB test suite...
    call "%~dp0build.bat"
    if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%
)

echo Running AetherDB Verification Test Suite...
java -cp "%~dp0bin" com.aetherdb.test.TestRunner
