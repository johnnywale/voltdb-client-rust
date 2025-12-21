.PHONY: build release test test-unit test-async test-all check fix clean docker-run docker-stop

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
