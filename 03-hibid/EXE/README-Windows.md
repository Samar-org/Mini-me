# Hibid Image Downloader — Team-friendly Packaging

This folder gives you two easy ways to share and run **Hibid Image Downloader** without Python installed.

## Option A — Single EXE for Windows (recommended)
1. On **one** Windows machine that *does* have Python 3.12+:
   - Open **PowerShell** in this folder and run:  
     ```powershell
     ./build.ps1
     ```
   - When it finishes, you'll get `dist/HibidImageDownloader.exe`.
2. Zip and share that `.exe` plus a **.env** file with your team. They will **not** need Python.
3. Teammates:
   - Put `.env` in the same folder as the `.exe`.
   - Double‑click `run-HibidDownloader.bat` (or run the `.exe` directly).

> The app reads `.env` from the *current working directory*. Keep `.env` next to the `.exe`.

### Schedule it on Windows (Task Scheduler)
1. Open **Task Scheduler** → **Create Task…**
2. **Actions** → *Start a program* → Program/script: point to the `.exe`.
3. **Start in**: the folder that contains the `.exe` and `.env` (important).
4. Set your **Triggers** (Daily, Weekly, etc.).

## Option B — Docker (cross‑platform)
If your team uses Docker:
```bash
docker build -t hibid-downloader .
docker run --rm -it --env-file .env -v "%cd%/output:/app/output" hibid-downloader
```
- The container will write images under `/app/output` (mapped to `./output` on your machine).
- You can pass a custom Airtable view name:
  ```bash
  docker run --rm -it --env-file .env hibid-downloader "Hibid File Loading View"
  ```

## Environment (.env) — required
Copy `.env.example` to `.env` and fill values:
```
AIRTABLE_API_KEY=key_...
AIRTABLE_BASE_ID=app_...
AIRTABLE_TABLE_NAME=Items-Bid4more
AIRTABLE_VIEW_NAME=Hibid File Loading View
OUTPUT_DIR=downloaded_images
MAX_WIDTH=800
MAX_HEIGHT=800
```

> Only `AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID` are strictly required. Others have defaults.

## Notes
- The program resizes/canvases images to the configured `MAX_WIDTH` × `MAX_HEIGHT` (default 800×800).
- File naming is **always** based on `"Hibid Lot No"` and follows your conditional field order logic.
- A CSV download report is generated when images are saved.
