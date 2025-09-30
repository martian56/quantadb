# QuantaDB Build Script for Windows

Write-Host "🚀 Building QuantaDB..." -ForegroundColor Green

# Build the server
Write-Host "📦 Building server..." -ForegroundColor Yellow
Set-Location server
cargo build --release
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Server build failed" -ForegroundColor Red
    exit 1
}
Set-Location ..

# Build the Rust client
Write-Host "📦 Building Rust client..." -ForegroundColor Yellow
Set-Location connectors/rust-client
cargo build --release
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Rust client build failed" -ForegroundColor Red
    exit 1
}
Set-Location ../..

# Build the Python client
Write-Host "📦 Building Python client..." -ForegroundColor Yellow
Set-Location connectors/python-client
maturin build --release
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Python client build failed" -ForegroundColor Red
    exit 1
}
Set-Location ../..

# Build the desktop client
Write-Host "📦 Building desktop client..." -ForegroundColor Yellow
Set-Location client
cargo tauri build
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Desktop client build failed" -ForegroundColor Red
    exit 1
}
Set-Location ..

Write-Host "✅ All builds completed successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "📁 Build artifacts:" -ForegroundColor Cyan
Write-Host "  Server: server/target/release/quanta-server.exe" -ForegroundColor White
Write-Host "  Rust Client: connectors/rust-client/target/release/libquanta_client.rlib" -ForegroundColor White
Write-Host "  Python Client: connectors/python-client/target/wheels/" -ForegroundColor White
Write-Host "  Desktop Client: client/src-tauri/target/release/bundle/" -ForegroundColor White
