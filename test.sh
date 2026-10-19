#!/usr/bin/env bash
set -e

if [ ! -f "bin/com/aetherdb/test/TestRunner.class" ]; then
    echo "Building AetherDB test suite..."
    ./build.sh
fi

echo "Running AetherDB Verification Test Suite..."
java -cp bin com.aetherdb.test.TestRunner
