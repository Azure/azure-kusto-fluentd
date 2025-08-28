.PHONY: all build install clean lint test unit-test e2e-test coverage doc

# Default target
all: build test lint

# Install dependencies
build:
	@echo "Installing dependencies..."
	bundle install

# Install the gem locally
install: build
	@echo "Installing gem locally..."
	bundle exec rake install

# Remove build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf *.gem
	rm -rf .yardoc
	rm -rf coverage
	rm -rf doc
	rm -rf pkg
	rm -rf tmp

# Run Rubocop for linting
lint:
	@echo "Running Rubocop..."
	bundle exec rubocop

# Fix auto-correctable Rubocop issues
lint-fix:
	@echo "Auto-fixing Rubocop issues..."
	bundle exec rubocop -A

# Run all tests
test: unit-test

# Run unit tests
unit-test:
	@echo "Running unit tests..."
	bundle exec rake test

# Run specific unit test file
test-file:
	@echo "Running specific test file: $(FILE)"
	bundle exec rake test TEST="$(FILE)"

# Run end-to-end tests with Azure
e2e-test:
	@echo "Running end-to-end tests with Azure Kusto..."
	@echo "This requires Azure credentials to be set as environment variables."
	bundle exec rake test TEST="test/plugin/test_e2e_kusto.rb"

# Run specific e2e test
e2e-test-single:
	@echo "Running specific e2e test: $(TEST_NAME)"
	bundle exec rake test TEST="test/plugin/test_e2e_kusto.rb" TESTOPTS="--name=$(TEST_NAME)"

# Generate code coverage report
coverage:
	@echo "Generating code coverage report..."
	COVERAGE=true bundle exec rake test

# Generate documentation
doc:
	@echo "Generating documentation..."
	bundle exec yard doc

# Show help
help:
	@echo "Available targets:"
	@echo "  all        - Run build, test, and lint"
	@echo "  build      - Install dependencies"
	@echo "  install    - Install the gem locally"
	@echo "  clean      - Remove build artifacts"
	@echo "  lint       - Run Rubocop for linting"
	@echo "  lint-fix   - Fix auto-correctable Rubocop issues"
	@echo "  test       - Run all tests"
	@echo "  unit-test  - Run unit tests"
	@echo "  test-file  - Run specific test file (usage: make test-file FILE=test/file_path.rb)"
	@echo "  e2e-test   - Run end-to-end tests with Azure Kusto"
	@echo "  e2e-test-single - Run specific e2e test (usage: make e2e-test-single TEST_NAME='test name')"
	@echo "  coverage   - Generate code coverage report"
	@echo "  doc        - Generate documentation"