# Building RPM Package for Oracle Linux 8+

This directory contains the RPM spec file and build tools for creating an RPM package of the generate-synthetic-data utility.

## Prerequisites

### For Oracle Linux 8+

```bash
# Install RPM build tools
sudo dnf install -y rpm-build rpmdevtools

# Install build dependencies
sudo dnf install -y python3-devel python3-PyMySQL

# Install runtime dependencies (optional, for testing)
sudo dnf install -y python3 python3-PyMySQL
```

Note: `python3-PyMySQL` is required as a build dependency because the RPM build process runs unit tests in the `%check` section.

## Quick Start

### Build the RPM

```bash
# Build both binary and source RPM
make rpm

# Or build just the source RPM
make srpm
```

### Install the RPM

```bash
# After building, install the package
sudo rpm -ivh ~/rpmbuild/RPMS/noarch/generate-synthetic-data-1.0.0-1.*.noarch.rpm
```

### Use the installed tool

```bash
# The tool will be available as 'generate-synthetic-data' command
generate-synthetic-data --help
```

## Manual Build Process

If you prefer to build manually without the Makefile:

### 1. Setup RPM build environment

```bash
rpmdev-setuptree
```

This creates the directory structure at `~/rpmbuild/`:
- `BUILD/` - Build directory
- `RPMS/` - Binary RPMs
- `SOURCES/` - Source tarballs
- `SPECS/` - Spec files
- `SRPMS/` - Source RPMs

### 2. Create source tarball

```bash
# Create tarball with required files
tar czf generate-synthetic-data-1.0.0.tar.gz \
    --transform 's,^,generate-synthetic-data-1.0.0/,' \
    generate_synthetic_data.py \
    generate_synthetic_data_utils.py \
    generate_synthetic_data_patterns.py \
    constraint_resolver.py \
    schema_introspector.py \
    value_generator.py \
    README.md \
    CARTESIAN_UNIQUE_FK_FEATURE.md \
    MULTI_CONSTRAINT_CARTESIAN_FEATURE.md \
    REFACTORING.md
```

### 3. Copy files to RPM build directories

```bash
cp generate-synthetic-data-1.0.0.tar.gz ~/rpmbuild/SOURCES/
cp generate-synthetic-data.spec ~/rpmbuild/SPECS/
```

### 4. Build the RPM

```bash
# Build both source and binary RPM
rpmbuild -ba ~/rpmbuild/SPECS/generate-synthetic-data.spec

# Or build just binary RPM
rpmbuild -bb ~/rpmbuild/SPECS/generate-synthetic-data.spec

# Or build just source RPM
rpmbuild -bs ~/rpmbuild/SPECS/generate-synthetic-data.spec
```

## Package Contents

After installation, the package provides:

### Executable
- `/usr/bin/generate-synthetic-data` - Main command-line tool

### Python Modules
- `/usr/share/generate-synthetic-data/generate_synthetic_data.py` - Main script
- `/usr/share/generate-synthetic-data/generate_synthetic_data_utils.py` - Utility functions
- `/usr/share/generate-synthetic-data/generate_synthetic_data_patterns.py` - Pattern optimizations
- `/usr/share/generate-synthetic-data/constraint_resolver.py` - Constraint resolution logic
- `/usr/share/generate-synthetic-data/schema_introspector.py` - Schema introspection
- `/usr/share/generate-synthetic-data/value_generator.py` - Value generation

### Documentation
- `/usr/share/doc/generate-synthetic-data/README.md` - Main documentation
- `/usr/share/doc/generate-synthetic-data/CARTESIAN_UNIQUE_FK_FEATURE.md` - Cartesian features
- `/usr/share/doc/generate-synthetic-data/MULTI_CONSTRAINT_CARTESIAN_FEATURE.md` - Multi-constraint features
- `/usr/share/doc/generate-synthetic-data/REFACTORING.md` - Refactoring notes

## Build-time Testing

The RPM spec file includes a `%check` section that runs all unit tests during the build process. This ensures:

- All 20 unit test suites are executed
- Tests must pass for the RPM build to succeed
- Any test failures will prevent package creation
- Build log contains full test output for debugging

The tests validate:
- Constraint resolution logic
- Value generation algorithms
- FK relationship handling
- UNIQUE constraint satisfaction
- Data type conversions
- Configuration parsing

This provides quality assurance and prevents regression issues in the packaged software.

## Verifying the Installation

After installing the RPM:

```bash
# Check installed files
rpm -ql generate-synthetic-data

# Check package information
rpm -qi generate-synthetic-data

# Verify the tool works
generate-synthetic-data --help
```

## Uninstalling

```bash
sudo rpm -e generate-synthetic-data
```

## Makefile Targets

- `make help` - Show available targets
- `make rpm` - Build binary RPM (default)
- `make srpm` - Build source RPM
- `make tarball` - Create source tarball only
- `make setup-dirs` - Setup RPM build directories
- `make clean` - Remove build artifacts

## Troubleshooting

### Missing build tools

If `rpmbuild` is not found:
```bash
sudo dnf install rpm-build
```

### Missing dependencies

If Python dependencies are missing:
```bash
sudo dnf install python3-devel python3-PyMySQL
```

### Permission issues

If you can't write to `/usr/bin` or `/usr/share`:
```bash
# Build as regular user
make rpm

# Install as root
sudo rpm -ivh ~/rpmbuild/RPMS/noarch/generate-synthetic-data-*.rpm
```

## Notes for Oracle Linux 8

- Python 3.6 or higher is required (Oracle Linux 8 includes Python 3.6+)
- PyMySQL can be installed via `python3-PyMySQL` package or `pip3 install PyMySQL`
- The package is architecture-independent (noarch)
- Works on Oracle Linux 8, 9, and compatible RHEL-based distributions

## Version Information

- Package Version: 1.0.0
- Release: 1
- License: MIT
- Architecture: noarch
- Target OS: Oracle Linux 8+
