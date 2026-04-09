@echo off
setlocal

for /f "tokens=2 delims=:." %%a in ('chcp') do set "OLD_CP=%%a"
set "OLD_CP=%OLD_CP: =%"
chcp 65001 >nul

cd /d "%~dp0"

if not exist "dicom_cleaner.exe" (
echo dicom_cleaner.exe not found
pause
exit /b 1
)

set "JOBS=2"
set "LOG_DIR=logs"
set "STATE_FILE=.dicom_cleanup_state.json"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

set "TARGET=%~1"
if "%TARGET%"=="" (
set /p TARGET=Input folder path, for example E:\DICOM :
)

if "%TARGET%"=="" (
echo No folder path provided
pause
exit /b 1
)

if not exist "%TARGET%" (
echo Folder not found: %TARGET%
pause
exit /b 1
)

for /f "tokens=1-3 delims=/:.- " %%a in ("%date%") do set "DATESTAMP=%%a-%%b-%%c"
for /f "tokens=1-3 delims=:., " %%a in ("%time%") do set "TIMESTAMP=%%a-%%b-%%c"
set "TIMESTAMP=%TIMESTAMP: =0%"
set "LOG_FILE=%LOG_DIR%\dicom_cleaner_%DATESTAMP%_%TIMESTAMP%.log"

echo.
echo Target: %TARGET%
echo Jobs: %JOBS%
echo State file: %STATE_FILE%
echo Log file: %LOG_FILE%
echo.

"%~dp0dicom_cleaner.exe" "%TARGET%" --incremental --state "%STATE_FILE%" -j %JOBS% > "%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

type "%LOG_FILE%"
echo.
echo Exit code: %EXIT_CODE%
echo.

if not "%OLD_CP%"=="" chcp %OLD_CP% >nul
pause
exit /b %EXIT_CODE%
