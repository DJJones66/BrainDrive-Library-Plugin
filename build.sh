#!/bin/bash

set -euo pipefail

echo "Building BrainDriveLibraryPlugin..."

if [ ! -d "node_modules" ]; then
  echo "Installing dependencies..."
  npm install
fi

echo "Cleaning previous build..."
npm run clean

echo "Building plugin..."
npm run build

if [ -f "dist/remoteEntry.js" ]; then
  echo "Build successful: dist/remoteEntry.js"
else
  echo "Build failed: dist/remoteEntry.js missing"
  exit 1
fi
