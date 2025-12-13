@echo off
REM MongoDB Test Setup for Windows
REM This script helps set up MongoDB for PyMongoSQL testing

echo ================================
echo MongoDB Test Setup for PyMongoSQL
echo ================================
echo.

REM Check if Docker is available
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Docker not found!
    goto :alternatives
)

REM Check if Docker Desktop is running
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo Docker Desktop is not running!
    echo Please start Docker Desktop and try again.
    goto :alternatives
)

echo Docker is available and running.
echo.

REM Ask user what they want to do
echo What would you like to do?
echo 1. Start MongoDB test container
echo 2. Stop MongoDB test container  
echo 3. Check MongoDB status
echo 4. Setup test data
echo 5. Show alternatives
echo.

set /p choice="Enter choice (1-5): "

if "%choice%"=="1" goto :start
if "%choice%"=="2" goto :stop
if "%choice%"=="3" goto :status
if "%choice%"=="4" goto :setup
if "%choice%"=="5" goto :alternatives
goto :invalid

:start
echo Starting MongoDB test container...
python mongo_test_helper.py start
goto :end

:stop
echo Stopping MongoDB test container...
python mongo_test_helper.py stop
goto :end

:status
echo Checking MongoDB status...
python mongo_test_helper.py status
goto :end

:setup
echo Setting up test data...
python mongo_test_helper.py setup
goto :end

:alternatives
echo.
echo ================================
echo MongoDB Installation Alternatives
echo ================================
python mongo_test_helper.py alternatives
goto :end

:invalid
echo Invalid choice. Please run the script again.
goto :end

:end
echo.
echo Press any key to exit...
pause >nul