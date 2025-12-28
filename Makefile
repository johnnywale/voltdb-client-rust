.PHONY: build release test test-unit test-async test-all check fix clean docker-run docker-stop \
        coverage coverage-html coverage-lcov coverage-json coverage-tarpaulin coverage-tarpaulin-xml \
        coverage-tarpaulin-lcov install-coverage-tools

# Build targets
build:
	cargo build

build-tokio:
	cargo build --features tokio

release:
	cargo build --release

release-tokio:
	cargo build --release --features tokio

# Test targets
test:
	cargo test --verbose

test-lib:
	cargo test --lib

test-lib-tokio:
	cargo test --features tokio --lib

test-unit:
	cargo test --test unit_test

test-async:
	cargo test --features tokio --test async_test

test-integration:
	cargo test --test integration_test

test-pool:
	cargo test --test pool_test

test-all:
	cargo test --features tokio --verbose

test-all-no-docker:
	cargo test --features tokio --lib
	cargo test --features tokio --test unit_test

# Code quality
check:
	cargo check
	cargo check --features tokio

fix:
	cargo fix

fmt:
	cargo fmt

clippy:
	cargo clippy
	cargo clippy --features tokio

# Coverage (requires cargo-llvm-cov or cargo-tarpaulin)
# Install: cargo install cargo-llvm-cov
# Or: cargo install cargo-tarpaulin (Linux only)

# Basic coverage - lib tests only (no Docker needed)
coverage:
	cargo llvm-cov --lib --features tokio

# Coverage with unit tests (no Docker needed)
coverage-unit:
	cargo llvm-cov --features tokio --lib --test unit_test

coverage-unit-html:
	cargo llvm-cov --features tokio --lib --test unit_test --html
	@echo "Coverage report: target/llvm-cov/html/index.html"

# Full coverage - all tests (requires Docker)
coverage-all:
	cargo llvm-cov --features tokio --lib --test unit_test --test integration_test --test pool_test --test async_test

coverage-all-html:
	cargo llvm-cov --features tokio --lib --test unit_test --test integration_test --test pool_test --test async_test --html
	@echo "Coverage report: target/llvm-cov/html/index.html"

coverage-html:
	cargo llvm-cov --lib --features tokio --html
	@echo "Coverage report: target/llvm-cov/html/index.html"

coverage-lcov:
	cargo llvm-cov --lib --features tokio --lcov --output-path target/lcov.info

coverage-json:
	cargo llvm-cov --lib --features tokio --json --output-path target/coverage.json

# Tarpaulin (Linux only, often used in CI)
coverage-tarpaulin:
	cargo tarpaulin --features tokio --out Html --output-dir target/tarpaulin -- --test-threads=1
	@echo "Coverage report: target/tarpaulin/tarpaulin-report.html"

coverage-tarpaulin-xml:
	cargo tarpaulin --features tokio --out Xml --output-dir target/tarpaulin -- --test-threads=1

coverage-tarpaulin-lcov:
	cargo tarpaulin --features tokio --out Lcov --output-dir target/tarpaulin -- --test-threads=1

# Coverage without Docker (lib + unit tests only)
coverage-tarpaulin-no-docker:
	cargo tarpaulin --lib --features tokio --test unit_test --out Html --output-dir target/tarpaulin
	@echo "Coverage report: target/tarpaulin/tarpaulin-report.html"

# Install coverage tools
install-coverage-tools:
	cargo install cargo-llvm-cov
	@echo "Note: cargo-tarpaulin only works on Linux"
	@echo "On Linux, also run: cargo install cargo-tarpaulin"

# Clean
clean:
	cargo clean

# Docker targets
docker-run:
	docker run --env HOST_COUNT=1 --publish 21211:21211 --publish 8080:8080 basvanbeek/voltdb-community:9.2.1

docker-run-detached:
	docker run -d --env HOST_COUNT=1 --publish 21211:21211 --publish 8080:8080 basvanbeek/voltdb-community:9.2.1

docker-stop:
	docker stop $$(docker ps -q --filter ancestor="basvanbeek/voltdb-community:9.2.1")

docker-rm:
	docker rm $$(docker ps -aq --filter ancestor="basvanbeek/voltdb-community:9.2.1")

# Cross-compilation (Linux from macOS)
install-linux-target:
	rustup target add x86_64-unknown-linux-gnu

build-linux:
	CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-unknown-linux-gnu-gcc cargo build --release --target=x86_64-unknown-linux-gnu

prepare-cross-compile:
	rustup target add x86_64-unknown-linux-gnu
	brew tap SergioBenitez/osxct
	brew install x86_64-unknown-linux-gnu

# Rust toolchain
upgrade-rust:
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Help
help:
	@echo "Available targets:"
	@echo ""
	@echo "Build:"
	@echo "  build            - Build the library (sync only)"
	@echo "  build-tokio      - Build with tokio async support"
	@echo "  release          - Build release version"
	@echo "  release-tokio    - Build release with tokio"
	@echo ""
	@echo "Test:"
	@echo "  test             - Run all tests (requires Docker)"
	@echo "  test-lib         - Run library tests only"
	@echo "  test-lib-tokio   - Run library tests with tokio"
	@echo "  test-unit        - Run unit tests (no Docker needed)"
	@echo "  test-async       - Run async tests (requires Docker)"
	@echo "  test-integration - Run integration tests (requires Docker)"
	@echo "  test-pool        - Run pool tests (requires Docker)"
	@echo "  test-all         - Run all tests with tokio feature"
	@echo "  test-all-no-docker - Run tests that don't need Docker"
	@echo ""
	@echo "Coverage (install: cargo install cargo-llvm-cov):"
	@echo "  coverage              - Lib tests only (no Docker)"
	@echo "  coverage-unit         - Lib + unit tests (no Docker)"
	@echo "  coverage-unit-html    - HTML report (no Docker)"
	@echo "  coverage-all          - All tests (requires Docker)"
	@echo "  coverage-all-html     - HTML report (requires Docker)"
	@echo "  coverage-html         - Lib tests HTML report"
	@echo "  coverage-lcov         - Generate LCOV format (for CI)"
	@echo "  coverage-json         - Generate JSON format"
	@echo "  coverage-tarpaulin    - Use tarpaulin (Linux, requires Docker)"
	@echo "  coverage-tarpaulin-no-docker - Tarpaulin without Docker"
	@echo "  install-coverage-tools - Install coverage tools"
	@echo ""
	@echo "Code Quality:"
	@echo "  check            - Check code compiles"
	@echo "  fix              - Auto-fix code issues"
	@echo "  fmt              - Format code"
	@echo "  clippy           - Run clippy linter"
	@echo ""
	@echo "Docker:"
	@echo "  docker-run       - Start VoltDB container (foreground)"
	@echo "  docker-run-detached - Start VoltDB container (background)"
	@echo "  docker-stop      - Stop VoltDB container"
	@echo "  docker-rm        - Remove VoltDB container"
	@echo ""
	@echo "Other:"
	@echo "  clean            - Clean build artifacts"
	@echo "  help             - Show this help"
