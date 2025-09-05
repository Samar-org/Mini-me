\
@echo off
REM Launch helper that ensures we run from the script folder so .env is found.
setlocal enabledelayedexpansion
cd /d "%~dp0"
if exist ".env" (
  echo Using .env in %CD%
) else (
  echo WARNING: .env not found in %CD%
)
.\HibidImageDownloader.exe %*
pause
