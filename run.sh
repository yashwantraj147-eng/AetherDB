#!/usr/bin/env bash
set -e

if [ ! -f "bin/com/aetherdb/AetherServer.class" ]; then
    echo "AetherDB is not built yet. Running build.sh..."
    ./build.sh
fi

echo "Starting AetherDB Server..."
java -cp bin com.aetherdb.AetherServer config/aether-cluster.properties
