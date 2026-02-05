# Quick Reference: RPM Package for Oracle Linux 8+

## Building the RPM

```bash
# One command to build
make rpm
```

## Installing

```bash
# Install the package
sudo rpm -ivh ~/rpmbuild/RPMS/noarch/generate-synthetic-data-1.0.0-1.*.noarch.rpm
```

## Using

```bash
# Show help
generate-synthetic-data --help

# Basic usage
generate-synthetic-data \
  --config config.json \
  --src-host localhost \
  --src-user root \
  --out-sql output.sql
```

## Package Details

- **Name**: generate-synthetic-data
- **Version**: 1.0.0
- **Architecture**: noarch
- **Python**: 3.6+
- **Dependency**: python3-PyMySQL

## Files Installed

```
/usr/bin/generate-synthetic-data
/usr/share/generate-synthetic-data/
  ├── generate_synthetic_data.py
  ├── generate_synthetic_data_utils.py
  ├── generate_synthetic_data_patterns.py
  ├── constraint_resolver.py
  ├── schema_introspector.py
  └── value_generator.py
/usr/share/doc/generate-synthetic-data/
  ├── README.md
  ├── CARTESIAN_UNIQUE_FK_FEATURE.md
  ├── MULTI_CONSTRAINT_CARTESIAN_FEATURE.md
  └── REFACTORING.md
```

## Makefile Targets

| Command | Description |
|---------|-------------|
| `make help` | Show available targets |
| `make rpm` | Build binary RPM (default) |
| `make srpm` | Build source RPM |
| `make tarball` | Create source tarball |
| `make setup-dirs` | Setup RPM build directories |
| `make clean` | Remove build artifacts |

## Prerequisites for Building

```bash
sudo dnf install -y rpm-build rpmdevtools python3-devel
```

## Prerequisites for Running

```bash
sudo dnf install -y python3 python3-PyMySQL
```

## Quick Test

```bash
# After installation
generate-synthetic-data --help
rpm -ql generate-synthetic-data
```

## Uninstalling

```bash
sudo rpm -e generate-synthetic-data
```

## Support

- Oracle Linux 8, 9
- RHEL 8, 9
- Rocky Linux 8, 9
- AlmaLinux 8, 9
