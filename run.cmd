@echo off
setlocal
cd /d %~dp0

echo === MEXC Lag Monitor ===
echo Working directory: %CD%
echo.

if not exist config.json (
  echo ERROR: config.json not found in %CD%
  pause
  exit /b 1
)

if not exist target\release\mexc_lag.exe (
  echo Release binary not found. Building...
  cargo build --release || (
    echo Build failed.
    pause
    exit /b 1
  )
)

target\release\mexc_lag.exe
pause
