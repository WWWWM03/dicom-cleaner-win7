@echo off
for /f "tokens=2 delims=:." %%a in ('chcp') do set "OLD_CP=%%a"
set "OLD_CP=%OLD_CP: =%"
chcp 65001 >nul

cd /d "%~dp0"

if not exist "dicom_cleaner.exe" (
echo dicom_cleaner.exe not found
pause
exit /b 1
)

set /p TARGET=Input folder path, for example E:\DICOM :

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

echo Running...
"%~dp0dicom_cleaner.exe" "%TARGET%" --incremental -j 2
echo Exit code: %ERRORLEVEL%
if not "%OLD_CP%"=="" chcp %OLD_CP% >nul
pause
