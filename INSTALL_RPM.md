# RPM Installation Guide for Oracle Linux 8+

This guide provides complete instructions for installing the generate-synthetic-data tool via RPM on Oracle Linux 8 and above.

## Quick Installation (Pre-built RPM)

If you have a pre-built RPM file:

```bash
# Install the package
sudo rpm -ivh generate-synthetic-data-1.0.0-1.el8.noarch.rpm

# Verify installation
generate-synthetic-data --help
```

## Building and Installing from Source

### Step 1: Install Build Dependencies

```bash
# Update system
sudo dnf update -y

# Install RPM build tools
sudo dnf install -y rpm-build rpmdevtools make

# Install Python development tools
sudo dnf install -y python3-devel

# Install runtime dependencies (optional, for testing)
sudo dnf install -y python3 python3-PyMySQL
```

### Step 2: Clone the Repository

```bash
git clone https://github.com/ccard-fpt/generate_synthetic_data.git
cd generate_synthetic_data
```

### Step 3: Build the RPM

```bash
# Build both source and binary RPM
make rpm

# This will create:
# - Binary RPM: ~/rpmbuild/RPMS/noarch/generate-synthetic-data-1.0.0-1.*.noarch.rpm
# - Source RPM: ~/rpmbuild/SRPMS/generate-synthetic-data-1.0.0-1.*.src.rpm
```

### Step 4: Install the RPM

```bash
# Install the binary RPM
sudo rpm -ivh ~/rpmbuild/RPMS/noarch/generate-synthetic-data-1.0.0-1.*.noarch.rpm
```

### Step 5: Verify Installation

```bash
# Check the installation
rpm -qa | grep generate-synthetic-data

# List installed files
rpm -ql generate-synthetic-data

# Test the tool
generate-synthetic-data --help
```

## Post-Installation Configuration

### Install PyMySQL (if not already installed)

The tool requires PyMySQL to connect to MySQL databases:

```bash
# Option 1: Using dnf (recommended for Oracle Linux)
sudo dnf install -y python3-PyMySQL

# Option 2: Using pip
pip3 install PyMySQL
```

### Test the Installation

Create a simple test configuration:

```bash
# Create a test directory
mkdir -p ~/generate-synthetic-data-test
cd ~/generate-synthetic-data-test

# Create a sample configuration file
cat > config.json << 'EOF'
[
  {
    "schema": "testdb",
    "table": "users",
    "rows": 100
  }
]
EOF

# Test the tool (will fail if MySQL is not accessible, but validates the tool works)
generate-synthetic-data --help
```

## Using the Tool

### Basic Usage

```bash
generate-synthetic-data \
  --config config.json \
  --src-host localhost \
  --src-user root \
  --out-sql output.sql
```

### With Debug Output

```bash
# Level 1: High-level output
generate-synthetic-data \
  --config config.json \
  --src-host localhost \
  --src-user root \
  --out-sql output.sql \
  --debug

# Level 3: Verbose output with timestamps
generate-synthetic-data \
  --config config.json \
  --src-host localhost \
  --src-user root \
  --out-sql output.sql \
  --debug-level 3
```

## Upgrading

To upgrade to a newer version:

```bash
# Remove old version
sudo rpm -e generate-synthetic-data

# Install new version
sudo rpm -ivh generate-synthetic-data-<new-version>.rpm
```

Or use upgrade directly:

```bash
sudo rpm -Uvh generate-synthetic-data-<new-version>.rpm
```

## Uninstalling

```bash
# Remove the package
sudo rpm -e generate-synthetic-data

# Verify removal
rpm -qa | grep generate-synthetic-data
```

## Troubleshooting

### Problem: Command not found

If `generate-synthetic-data` is not found after installation:

```bash
# Check if the package is installed
rpm -qa | grep generate-synthetic-data

# Check if the binary exists
ls -l /usr/bin/generate-synthetic-data

# Verify PATH includes /usr/bin
echo $PATH
```

### Problem: PyMySQL not found

If you get "Error: PyMySQL required":

```bash
# Install PyMySQL
sudo dnf install -y python3-PyMySQL

# Or using pip
pip3 install PyMySQL
```

### Problem: Python import errors

If you get import errors for the tool's modules:

```bash
# Verify all files are installed
rpm -ql generate-synthetic-data

# Check the wrapper script
cat /usr/bin/generate-synthetic-data

# Verify Python path
python3 -c "import sys; print('\n'.join(sys.path))"
```

### Problem: Permission denied

If you get permission errors:

```bash
# Ensure the wrapper script is executable
sudo chmod +x /usr/bin/generate-synthetic-data

# Ensure Python script is readable
sudo chmod 755 /usr/share/generate-synthetic-data/generate_synthetic_data.py
```

## Package Information

### Installed Files

After installation, the package installs:

- **Executable**: `/usr/bin/generate-synthetic-data`
- **Python modules**: `/usr/share/generate-synthetic-data/*.py`
- **Documentation**: `/usr/share/doc/generate-synthetic-data/*.md`

### Package Dependencies

- Python 3.6 or higher (included in Oracle Linux 8)
- python3-PyMySQL (must be installed separately)

### Package Metadata

```bash
# View package information
rpm -qi generate-synthetic-data

# View package dependencies
rpm -qR generate-synthetic-data

# View package changelog
rpm -q --changelog generate-synthetic-data
```

## Support for Different Oracle Linux Versions

### Oracle Linux 8

Fully supported. Python 3.6+ is included by default.

```bash
# Check Python version
python3 --version

# Should show Python 3.6.x or higher
```

### Oracle Linux 9

Fully supported. Python 3.9+ is included by default.

```bash
# Check Python version
python3 --version

# Should show Python 3.9.x or higher
```

## Advanced Topics

### Building SRPM for Distribution

To build a source RPM for distribution:

```bash
# Build source RPM only
make srpm

# The SRPM will be at:
# ~/rpmbuild/SRPMS/generate-synthetic-data-1.0.0-1.*.src.rpm
```

Others can then build the binary RPM from your SRPM:

```bash
rpmbuild --rebuild generate-synthetic-data-1.0.0-1.*.src.rpm
```

### Installing to Custom Location

The RPM installs to standard system locations. If you need a custom installation:

```bash
# Extract RPM contents without installing
rpm2cpio generate-synthetic-data-1.0.0-1.*.noarch.rpm | cpio -idmv

# This extracts to ./usr/bin and ./usr/share directories
```

### Creating a Repository

To host RPMs in a local repository:

```bash
# Install createrepo
sudo dnf install -y createrepo

# Create repository directory
mkdir -p /var/www/html/rpms

# Copy your RPM
cp ~/rpmbuild/RPMS/noarch/generate-synthetic-data-*.rpm /var/www/html/rpms/

# Create repository metadata
createrepo /var/www/html/rpms
```

## Getting Help

- View built-in help: `generate-synthetic-data --help`
- Documentation: `/usr/share/doc/generate-synthetic-data/`
- Project repository: https://github.com/ccard-fpt/generate_synthetic_data
