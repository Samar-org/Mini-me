\
# Build a single-file Windows EXE using PyInstaller
# Run in PowerShell from the same folder as v3-Hibid-Images_Downloader.py

$ErrorActionPreference = "Stop"

# 1) Create isolated venv
if (-Not (Test-Path ".venv")) {
    python -m venv .venv
}

# 2) Activate and upgrade pip
& .\.venv\Scripts\pip.exe install --upgrade pip

# 3) Install runtime deps
& .\.venv\Scripts\pip.exe install pillow requests python-dotenv pyairtable urllib3

# 4) Install PyInstaller
& .\.venv\Scripts\pip.exe install pyinstaller

# 5) Build single-file exe
# --collect-all ensures PIL plugins/fonts are bundled; hidden-import covers urllib3 Retry usage.
& .\.venv\Scripts\pyinstaller.exe `
    --onefile `
    --name HibidImageDownloader `
    --collect-all PIL `
    --hidden-import urllib3.util.retry `
    --clean `
    v3-Hibid-Images_Downloader.py

Write-Host ""
Write-Host "✅ Build complete. Find your EXE at: dist\HibidImageDownloader.exe" -ForegroundColor Green
