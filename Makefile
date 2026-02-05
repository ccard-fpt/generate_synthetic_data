# Makefile for building generate-synthetic-data RPM

NAME = generate-synthetic-data
VERSION = 1.0.0
RELEASE = 1

# RPM build directories
RPMBUILD_DIR = $(HOME)/rpmbuild
SPEC_FILE = $(NAME).spec
TARBALL = $(NAME)-$(VERSION).tar.gz

# Source files to include in tarball
SOURCES = \
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

.PHONY: all clean rpm srpm tarball setup-dirs help

help:
	@echo "Available targets:"
	@echo "  rpm        - Build binary RPM package"
	@echo "  srpm       - Build source RPM package"
	@echo "  tarball    - Create source tarball"
	@echo "  setup-dirs - Setup RPM build directory structure"
	@echo "  clean      - Remove build artifacts"
	@echo "  help       - Show this help message"

setup-dirs:
	@echo "Setting up RPM build directories..."
	@mkdir -p $(RPMBUILD_DIR)/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
	@echo "RPM build directories created at $(RPMBUILD_DIR)"

tarball: $(SOURCES)
	@echo "Creating source tarball..."
	@mkdir -p $(NAME)-$(VERSION)
	@cp -r $(SOURCES) $(NAME)-$(VERSION)/
	@tar czf $(TARBALL) $(NAME)-$(VERSION)
	@rm -rf $(NAME)-$(VERSION)
	@echo "Source tarball created: $(TARBALL)"

rpm: setup-dirs tarball
	@echo "Building RPM package..."
	@cp $(TARBALL) $(RPMBUILD_DIR)/SOURCES/
	@cp $(SPEC_FILE) $(RPMBUILD_DIR)/SPECS/
	@rpmbuild -ba $(RPMBUILD_DIR)/SPECS/$(SPEC_FILE)
	@echo "RPM build complete!"
	@echo "Binary RPM: $(RPMBUILD_DIR)/RPMS/noarch/$(NAME)-$(VERSION)-$(RELEASE).*.noarch.rpm"
	@echo "Source RPM: $(RPMBUILD_DIR)/SRPMS/$(NAME)-$(VERSION)-$(RELEASE).*.src.rpm"

srpm: setup-dirs tarball
	@echo "Building source RPM package..."
	@cp $(TARBALL) $(RPMBUILD_DIR)/SOURCES/
	@cp $(SPEC_FILE) $(RPMBUILD_DIR)/SPECS/
	@rpmbuild -bs $(RPMBUILD_DIR)/SPECS/$(SPEC_FILE)
	@echo "Source RPM created: $(RPMBUILD_DIR)/SRPMS/$(NAME)-$(VERSION)-$(RELEASE).*.src.rpm"

clean:
	@echo "Cleaning build artifacts..."
	@rm -f $(TARBALL)
	@rm -rf $(NAME)-$(VERSION)
	@echo "Clean complete!"

all: rpm
