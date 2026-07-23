$ErrorActionPreference = "Stop"

Push-Location $PSScriptRoot
try {
    Write-Host "Building QuantaDB workspace..." -ForegroundColor Green
    cargo build --workspace --release
    if ($LASTEXITCODE -ne 0) {
        throw "Cargo build failed with exit code $LASTEXITCODE"
    }

    Write-Host "Build completed successfully." -ForegroundColor Green
    Write-Host "Server: target/release/quantadb-server.exe" -ForegroundColor Cyan
}
finally {
    Pop-Location
}
