#!/usr/bin/env bash
set -euo pipefail

echo "========================================================"
echo "  AetherDB Verification & Regression Test Suite"
echo "========================================================"

if command -v mvn &> /dev/null; then
    mvn clean test
    exit $?
fi

echo "[ERROR] Maven executable (mvn) not found in PATH!"
exit 1
