#!/usr/bin/env bash
set -euo pipefail

echo "========================================================"
echo "  AetherDB Build Script - Maven Java 21 Canonical Build"
echo "========================================================"

if command -v mvn &> /dev/null; then
    mvn clean package -DskipTests
    exit $?
fi

echo "[ERROR] Maven executable (mvn) not found in PATH! Please install Apache Maven 3.9+."
exit 1
