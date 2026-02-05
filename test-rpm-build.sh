#!/bin/bash
# Test script to validate the RPM spec file

set -e

echo "=================================="
echo "RPM Spec File Validation Test"
echo "=================================="
echo ""

SPEC_FILE="generate-synthetic-data.spec"

# Check if spec file exists
if [ ! -f "$SPEC_FILE" ]; then
    echo "ERROR: Spec file not found: $SPEC_FILE"
    exit 1
fi
echo "✓ Spec file found: $SPEC_FILE"

# Check if rpmbuild is available (optional)
if command -v rpmbuild &> /dev/null; then
    echo "✓ rpmbuild is available"
    
    # Parse the spec file (syntax check)
    echo ""
    echo "Checking spec file syntax..."
    if rpmbuild --parse "$SPEC_FILE" &> /dev/null; then
        echo "✓ Spec file syntax is valid"
    else
        echo "⚠ Warning: Spec file may have syntax issues"
        rpmbuild --parse "$SPEC_FILE" 2>&1 | head -10
    fi
else
    echo "⚠ rpmbuild not available (optional for testing)"
fi

# Verify Makefile exists
if [ -f "Makefile" ]; then
    echo "✓ Makefile found"
else
    echo "⚠ Warning: Makefile not found"
fi

# Check if source files exist
echo ""
echo "Checking source files..."
REQUIRED_FILES=(
    "generate_synthetic_data.py"
    "generate_synthetic_data_utils.py"
    "generate_synthetic_data_patterns.py"
    "constraint_resolver.py"
    "schema_introspector.py"
    "value_generator.py"
    "README.md"
)

ALL_FOUND=true
for file in "${REQUIRED_FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✓ $file"
    else
        echo "  ✗ MISSING: $file"
        ALL_FOUND=false
    fi
done

if [ "$ALL_FOUND" = true ]; then
    echo "✓ All required source files present"
else
    echo "✗ Some source files are missing"
    exit 1
fi

# Test tarball creation
echo ""
echo "Testing tarball creation..."
if make tarball > /dev/null 2>&1; then
    if [ -f "generate-synthetic-data-1.0.0.tar.gz" ]; then
        echo "✓ Tarball created successfully"
        
        # List tarball contents
        echo ""
        echo "Tarball contents:"
        tar tzf generate-synthetic-data-1.0.0.tar.gz | sed 's/^/  /'
        
        # Check tarball size
        SIZE=$(du -h generate-synthetic-data-1.0.0.tar.gz | cut -f1)
        echo ""
        echo "✓ Tarball size: $SIZE"
    else
        echo "✗ Tarball was not created"
        exit 1
    fi
else
    echo "✗ Failed to create tarball"
    exit 1
fi

echo ""
echo "=================================="
echo "✓ All validation tests passed!"
echo "=================================="
echo ""
echo "Next steps:"
echo "1. Run 'make rpm' to build the RPM package"
echo "2. Install with: sudo rpm -ivh ~/rpmbuild/RPMS/noarch/generate-synthetic-data-*.rpm"
echo "3. Test with: generate-synthetic-data --help"
