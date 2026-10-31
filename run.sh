#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_FILE="$SCRIPT_DIR/target/aetherdb-2.0.2.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "[INFO] Packaging AetherDB executable jar..."
    "$SCRIPT_DIR/build.sh"
fi

echo "Starting AetherDB Server..."
exec java -jar "$JAR_FILE" "$SCRIPT_DIR/config/aether-cluster.properties"
