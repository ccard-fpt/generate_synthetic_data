#!/bin/bash
# Test script to simulate the %check section behavior

set -e

echo "=========================================="
echo "Testing %check section behavior"
echo "=========================================="
echo ""

# Simulate the environment that RPM build creates
BUILDDIR="/tmp/test-rpm-check-$$"
NAME="generate-synthetic-data"
VERSION="1.0.0"
SOURCE_DIR="$(pwd)"

echo "Creating test build directory: $BUILDDIR/$NAME-$VERSION"
mkdir -p "$BUILDDIR/$NAME-$VERSION"

# Copy necessary files to simulate extracted tarball
echo "Copying source files..."
cp *.py "$BUILDDIR/$NAME-$VERSION/" 2>/dev/null || true

cd "$BUILDDIR/$NAME-$VERSION"

echo ""
echo "Simulating %check section execution..."
echo "=========================================="

# This is what the %check section does
export PYTHONPATH="$BUILDDIR/$NAME-$VERSION:$PYTHONPATH"
echo "PYTHONPATH set to: $PYTHONPATH"
echo ""

TEST_COUNT=0
PASSED_COUNT=0
FAILED_TESTS=""

echo "Running unit tests..."
for test_file in test_*.py; do
    if [ -f "$test_file" ]; then
        TEST_COUNT=$((TEST_COUNT + 1))
        echo "Running $(basename $test_file)..."
        if python3 "$test_file" 2>&1; then
            PASSED_COUNT=$((PASSED_COUNT + 1))
            echo "  ✓ PASSED"
        else
            echo "  ✗ FAILED"
            FAILED_TESTS="$FAILED_TESTS $test_file"
        fi
        echo ""
    fi
done

echo "=========================================="
echo "Test Summary:"
echo "  Total tests: $TEST_COUNT"
echo "  Passed: $PASSED_COUNT"
echo "  Failed: $((TEST_COUNT - PASSED_COUNT))"

if [ -n "$FAILED_TESTS" ]; then
    echo "  Failed tests:$FAILED_TESTS"
    echo ""
    echo "✗ Some tests failed - RPM build would FAIL"
    cd "$SOURCE_DIR"
    rm -rf "$BUILDDIR"
    exit 1
else
    echo ""
    echo "✓ All tests passed successfully!"
    echo "✓ RPM build would succeed"
fi

# Cleanup
cd "$SOURCE_DIR"
rm -rf "$BUILDDIR"

echo ""
echo "=========================================="
echo "Validation complete!"
echo "=========================================="
