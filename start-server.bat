@echo off
title SHIV SONOGRAPHY & DIAGNOSTIC CENTRE - LAN Sync Server
color 0b
cls
echo ==============================================================================
echo       SHIV SONOGRAPHY & DIAGNOSTIC CENTRE - PHULERA (NWR EMPANELLED)
echo       LOCAL LAN REAL-TIME MULTI-COMPUTER SYNCHRONIZATION SERVER (MODE B)
echo ==============================================================================
echo.

cd /d "%~dp0"

echo [1/3] Verifying Node.js runtime environment...
where node >nul 2>nul
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Node.js is not found on this computer!
    echo To run the LAN Sync Server on this host machine, please install Node.js from:
    echo https://nodejs.org/ (LTS recommended)
    echo.
    pause
    exit /b 1
)

echo [2/3] Checking server dependencies...
if not exist "node_modules\express" (
    echo Installing required packages (express, cors, ws)...
    call npm install
    if %errorlevel% neq 0 (
        echo [ERROR] Dependency installation failed.
        pause
        exit /b 1
    )
)

echo [3/3] Launching Shiv Diagnostic LAN Real-Time Server...
echo.
node lan-server.js
echo.
pause
