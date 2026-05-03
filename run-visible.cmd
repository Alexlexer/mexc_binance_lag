@echo off
setlocal
cd /d %~dp0

echo === MEXC Lag Monitor ===
echo Working directory: %CD%
echo.

if not exist config.json (
  echo ERROR: config.json not found in %CD%
  echo.
  pause
  exit /b 1
)

if not exist target\release\mexc_lag.exe (
  echo Release binary not found. Building now...
  cargo build --release
  if errorlevel 1 (
    echo.
    echo Build failed.
    pause
    exit /b 1
  )
)

echo Starting bot. Press Ctrl+C to stop.
echo.
target\release\mexc_lag.exe

echo.
echo Bot stopped or crashed with exit code %ERRORLEVEL%.
pause
