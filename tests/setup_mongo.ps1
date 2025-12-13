# MongoDB Test Setup PowerShell Script
# Helps set up MongoDB for PyMongoSQL testing

function Show-Header {
    Write-Host "================================" -ForegroundColor Cyan
    Write-Host "MongoDB Test Setup for PyMongoSQL" -ForegroundColor Cyan  
    Write-Host "================================" -ForegroundColor Cyan
    Write-Host ""
}

function Test-Docker {
    try {
        $null = docker --version 2>$null
        if ($LASTEXITCODE -ne 0) { return $false }
        
        $null = docker info 2>$null
        return $LASTEXITCODE -eq 0
    }
    catch {
        return $false
    }
}

function Show-Menu {
    Write-Host "What would you like to do?" -ForegroundColor Yellow
    Write-Host "1. Start MongoDB test container"
    Write-Host "2. Stop MongoDB test container"
    Write-Host "3. Check MongoDB status"
    Write-Host "4. Setup test data"
    Write-Host "5. Run unit tests (mongomock)"
    Write-Host "6. Run integration tests (real MongoDB)"
    Write-Host "7. Show installation alternatives"
    Write-Host "8. Exit"
    Write-Host ""
}

function Start-MongoContainer {
    Write-Host "Starting MongoDB test container..." -ForegroundColor Green
    python mongo_test_helper.py start
}

function Stop-MongoContainer {
    Write-Host "Stopping MongoDB test container..." -ForegroundColor Red
    python mongo_test_helper.py stop
}

function Test-MongoStatus {
    Write-Host "Checking MongoDB status..." -ForegroundColor Blue
    python mongo_test_helper.py status
}

function Setup-TestData {
    Write-Host "Setting up test data..." -ForegroundColor Magenta
    python mongo_test_helper.py setup
}

function Run-UnitTests {
    Write-Host "Running unit tests with mongomock..." -ForegroundColor Green
    Set-Location ..
    python -m pytest tests/test_connection.py tests/test_cursor.py tests/test_result_set.py -v
    Set-Location tests
}

function Run-IntegrationTests {
    Write-Host "Running integration tests with real MongoDB..." -ForegroundColor Green
    Set-Location ..
    python -m pytest tests/test_integration_mongodb.py -v
    Set-Location tests
}

function Show-Alternatives {
    Write-Host ""
    Write-Host "================================" -ForegroundColor Cyan
    Write-Host "MongoDB Installation Alternatives" -ForegroundColor Cyan
    Write-Host "================================" -ForegroundColor Cyan
    python mongo_test_helper.py alternatives
}

# Main script
Show-Header

if (-not (Test-Docker)) {
    Write-Host "❌ Docker is not available or not running!" -ForegroundColor Red
    Write-Host ""
    Show-Alternatives
    Read-Host "Press Enter to exit"
    exit
}

Write-Host "✅ Docker is available and running." -ForegroundColor Green
Write-Host ""

do {
    Show-Menu
    $choice = Read-Host "Enter choice (1-8)"
    
    switch ($choice) {
        "1" { Start-MongoContainer }
        "2" { Stop-MongoContainer }
        "3" { Test-MongoStatus }
        "4" { Setup-TestData }
        "5" { Run-UnitTests }
        "6" { Run-IntegrationTests }
        "7" { Show-Alternatives }
        "8" { 
            Write-Host "Goodbye!" -ForegroundColor Green
            exit 
        }
        default { 
            Write-Host "Invalid choice. Please enter 1-8." -ForegroundColor Red 
        }
    }
    
    Write-Host ""
    Write-Host "Press Enter to continue..." -ForegroundColor Gray
    Read-Host
    Clear-Host
    Show-Header
} while ($true)