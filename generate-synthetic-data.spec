Name:           generate-synthetic-data
Version:        1.0.0
Release:        1%{?dist}
Summary:        Synthetic data generator for MySQL databases

License:        MIT
URL:            https://github.com/ccard-fpt/generate_synthetic_data
Source0:        %{name}-%{version}.tar.gz

BuildArch:      noarch
BuildRequires:  python3-devel
BuildRequires:  python3-PyMySQL
Requires:       python3 >= 3.6
Requires:       python3-PyMySQL

%description
A powerful Python tool for generating synthetic test data that respects
complex database constraints including foreign keys, composite UNIQUE
constraints, and polymorphic relationships.

Key features:
- Intelligent constraint handling
- Foreign key support (physical and logical)
- Static FK sampling
- Cartesian product generation
- Multi-threaded generation
- Reproducible results with seed-based random generation

%prep
%setup -q

%build
# Create wrapper script
cat > generate-synthetic-data-wrapper << 'EOF'
#!/bin/bash
# Wrapper script for generate-synthetic-data
export PYTHONPATH=/usr/share/generate-synthetic-data:$PYTHONPATH
exec /usr/bin/python3 /usr/share/generate-synthetic-data/generate_synthetic_data.py "$@"
EOF

%install
# Create installation directories
install -d %{buildroot}%{_bindir}
install -d %{buildroot}%{_datadir}/%{name}
install -d %{buildroot}%{_docdir}/%{name}

# Install wrapper script as main executable
install -m 755 generate-synthetic-data-wrapper %{buildroot}%{_bindir}/generate-synthetic-data

# Install main Python script
install -m 755 generate_synthetic_data.py %{buildroot}%{_datadir}/%{name}/

# Install Python modules
install -m 644 generate_synthetic_data_utils.py %{buildroot}%{_datadir}/%{name}/
install -m 644 generate_synthetic_data_patterns.py %{buildroot}%{_datadir}/%{name}/
install -m 644 constraint_resolver.py %{buildroot}%{_datadir}/%{name}/
install -m 644 schema_introspector.py %{buildroot}%{_datadir}/%{name}/
install -m 644 value_generator.py %{buildroot}%{_datadir}/%{name}/

# Install documentation
install -m 644 README.md %{buildroot}%{_docdir}/%{name}/
install -m 644 CARTESIAN_UNIQUE_FK_FEATURE.md %{buildroot}%{_docdir}/%{name}/
install -m 644 MULTI_CONSTRAINT_CARTESIAN_FEATURE.md %{buildroot}%{_docdir}/%{name}/
install -m 644 REFACTORING.md %{buildroot}%{_docdir}/%{name}/

%check
# Run unit tests
export PYTHONPATH=%{_builddir}/%{name}-%{version}:$PYTHONPATH
echo "Running unit tests..."
for test_file in %{_builddir}/%{name}-%{version}/test_*.py; do
    if [ -f "$test_file" ]; then
        echo "Running $(basename $test_file)..."
        %{__python3} "$test_file" || exit 1
    fi
done
echo "All tests passed successfully!"

%files
%{_bindir}/generate-synthetic-data
%{_datadir}/%{name}/
%doc %{_docdir}/%{name}/

%changelog
* Wed Feb 05 2026 Package Maintainer <maintainer@example.com> - 1.0.0-1
- Initial RPM release
- Support for Oracle Linux 8 and above
- Includes granular debug logging with timestamps
- Performance optimizations (regex pre-compilation, lock contention reduction)
- Added %check section to run unit tests during RPM build

