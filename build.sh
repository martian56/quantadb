#!/bin/bash

# QuantaDB Build Script for Unix-like systems

echo "🚀 Building QuantaDB..."

# Build the server
echo "📦 Building server..."
cd server
cargo build --release
if [ $? -ne 0 ]; then
    echo "❌ Server build failed"
    exit 1
fi
cd ..

# Build the Rust client
echo "📦 Building Rust client..."
cd connectors/rust-client
cargo build --release
if [ $? -ne 0 ]; then
    echo "❌ Rust client build failed"
    exit 1
fi
cd ../..

# Build the Python client
echo "📦 Building Python client..."
cd connectors/python-client
maturin build --release
if [ $? -ne 0 ]; then
    echo "❌ Python client build failed"
    exit 1
fi
cd ../..

# Build the desktop client
echo "📦 Building desktop client..."
cd client
cargo tauri build
if [ $? -ne 0 ]; then
    echo "❌ Desktop client build failed"
    exit 1
fi
cd ..

echo "✅ All builds completed successfully!"
echo ""
echo "📁 Build artifacts:"
echo "  Server: server/target/release/quanta-server"
echo "  Rust Client: connectors/rust-client/target/release/libquanta_client.rlib"
echo "  Python Client: connectors/python-client/target/wheels/"
echo "  Desktop Client: client/src-tauri/target/release/bundle/"
