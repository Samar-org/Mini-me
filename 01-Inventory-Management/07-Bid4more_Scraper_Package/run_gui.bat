@echo off
REM =============================================================================
REM Bid4more Product Scraper - GUI Launcher
REM This file is created automatically by setup.bat
REM =============================================================================

title Bid4more Product Scraper

REM Change to the directory where this batch file is located
cd /d "%~dp0"

REM Check if virtual environment exists
if not exist "venv\Scripts\activate.bat" (
    echo ERROR: Virtual environment not found.
    echo Please run setup.bat first to initialize the environment.
    echo.
    pause
    exit /b 1
)

REM Activate virtual environment
echo Activating Python environment...
call venv\Scripts\activate.bat

REM Check if the main GUI file exists
if not exist "scraper_gui.py" (
    echo ERROR: scraper_gui.py not found in current directory
    echo Please ensure all scraper files are present in the same folder.
    echo.
    pause
    exit /b 1
)

REM Check if .env file exists
if not exist ".env" (
    echo WARNING: .env configuration file not found
    echo Creating a template .env file...
    (
        echo # Airtable Configuration ^(Required^)
        echo AIRTABLE_API_KEY=YOUR_AIRTABLE_API_KEY_HERE
        echo AIRTABLE_BASE_ID=YOUR_AIRTABLE_BASE_ID_HERE
        echo.
        echo # Google Custom Search ^(Optional^)
        echo GOOGLE_API_KEY=YOUR_GOOGLE_API_KEY_HERE
        echo GOOGLE_CX=YOUR_CUSTOM_SEARCH_ENGINE_ID_HERE
        echo.
        echo # SerpAPI ^(Optional^)
        echo SERPAPI_KEY=YOUR_SERPAPI_KEY_HERE
    ) > .env
    echo.
    echo Please edit the .env file with your actual API keys before running the scraper.
    echo The .env file has been created in the current directory.
    echo.
    set /p choice="Would you like to continue anyway? (y/n): "
    if /i not "%choice%"=="y" (
        echo Please configure your API keys in the .env file and try again.
        pause
        exit /b 1
    )
)

REM Start the GUI application
echo Starting Bid4more Product Scraper GUI...
echo.
python scraper_gui.py

REM Check if the application started successfully
if %errorlevel% neq 0 (
    echo.
    echo ERROR: Failed to start the scraper GUI
    echo Error code: %errorlevel%
    echo.
    echo Possible causes:
    echo - Missing Python dependencies
    echo - Corrupted installation
    echo - Missing scraper files
    echo.
    echo Solutions:
    echo 1. Run setup.bat again to reinstall dependencies
    echo 2. Check that all .py files are in the current directory
    echo 3. Check the logs\ directory for detailed error information
    echo.
    pause
    exit /b 1
)

REM Application closed normally
echo.
echo Scraper GUI has been closed.
echo Check the logs\ directory for detailed operation logs.
echo.
pause