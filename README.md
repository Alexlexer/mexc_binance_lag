# MEXC Lag Monitor

Minimal monitor for checking whether Binance USD-M futures price moves lead MEXC futures on the same symbols.

## Windows Run

```powershell
cd C:\Dev\MexcLag
.\run.cmd
```

or directly:

```powershell
cd C:\Dev\MexcLag
.\target\release\mexc_lag.exe
```

## Linux/macOS Run From Source

```bash
cd MexcLag
chmod +x run.sh scripts/build-unix.sh
./run.sh
```

## Build Release Binaries For Linux/macOS/Windows

The repo includes `.github/workflows/build.yml`. Push this project to GitHub and run the `build-mexc-lag` workflow manually. It produces artifacts for:

- linux-x64
- macos-arm64
- macos-x64
- windows-x64

macOS binaries should be built on macOS runners; reliable macOS cross-compilation from Windows requires Apple SDK tooling.

## Output Files

- `lag_events.csv`: matched Binance -> MEXC lag events.
- `stats_snapshots.csv`: periodic per-symbol stats.
- `slippage_events.csv`: only matched events with MEXC bid/ask crossing, spread/slippage bps, gross bps, fee bps, and net bps.

## Config

Edit `config.json` to change symbols, thresholds, and fee assumptions. This is monitor-only; it does not use API keys and does not trade.
