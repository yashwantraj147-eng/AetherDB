#!/usr/bin/env bash
set -e

echo "========================================================"
echo "  AetherDB Build Script - Java 26 Native Compiler"
echo "========================================================"

mkdir -p bin

find src/main/java -name "*.java" > sources.txt

if [ ! -s sources.txt ]; then
    echo "Error: No Java source files found!"
    exit 1
fi

echo "Compiling AetherDB sources with javac..."
javac -d bin --release 26 @sources.txt

echo "Copying resources and web assets..."
mkdir -p bin/web bin/config
if [ -d "web" ]; then cp -r web/* bin/web/ 2>/dev/null || true; fi
if [ -d "config" ]; then cp -r config/* bin/config/ 2>/dev/null || true; fi

rm sources.txt

echo ""
echo "[BUILD SUCCESSFUL] AetherDB classes compiled into bin/"
echo "========================================================"
