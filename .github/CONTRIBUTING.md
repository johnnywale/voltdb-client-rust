# Contributing to voltdb-client-rust

Thank you for your interest in contributing! This document provides guidelines and information for contributors.

## Getting Started

### Prerequisites

- Rust 1.85.0 or later
- Docker (for running integration tests)
- Git

### Setup

1. Fork and clone the repository:
   ```bash
   git clone https://github.com/YOUR_USERNAME/voltdb-client-rust.git
   cd voltdb-client-rust
   ```

2. Start a VoltDB test container:
   ```bash
   docker run -d -p 21212:21212 -p 21211:21211 basvanbeek/voltdb-community:9.2.1
   ```

3. Run tests to verify your setup:
   ```bash
   cargo test --all-features
   ```

## Development Workflow

### Making Changes

1. Create a new branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes following the code style guidelines below

3. Run the test suite:
   ```bash
   cargo test --all-features
   ```

4. Run lints and formatting:
   ```bash
   cargo fmt
   cargo clippy --all-features -- -D warnings
   ```

5. Commit your changes:
   ```bash
   git commit -m "feat: add your feature description"
   ```

### Commit Message Format

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation only
- `refactor:` - Code refactoring
- `test:` - Adding or updating tests
- `ci:` - CI/CD changes
- `deps:` - Dependency updates

Example:
```
feat: add connection retry with exponential backoff

- Implement retry logic for transient failures
- Add configurable max retries and backoff multiplier
- Update documentation with retry examples

Closes #123
```

## Code Style Guidelines

### Rust Code

- Follow standard Rust formatting (`cargo fmt`)
- No clippy warnings (`cargo clippy -- -D warnings`)
- Write documentation comments for public APIs
- Use meaningful variable and function names
- Prefer `Result<T, E>` over panics

### Documentation

- All public items must have doc comments
- Include examples in doc comments where helpful
- Update README.md for significant API changes

### Testing

- Write unit tests for new functionality
- Integration tests go in `tests/` directory
- Use descriptive test names: `test_pool_reconnects_after_failure`
- Test both success and error cases

## Pull Request Process

1. Ensure all tests pass
2. Update documentation if needed
3. Add entry to CHANGELOG.md (if applicable)
4. Request review from maintainers
5. Address review feedback
6. Squash commits if requested

## Feature Requests and Bug Reports

- Use GitHub Issues with the provided templates
- Search existing issues before creating new ones
- Provide minimal reproduction cases for bugs

## Architecture Overview

```
src/
├── lib.rs           # Public exports, volt_param! macro
├── node.rs          # Sync TCP connection (Node)
├── pool.rs          # Sync connection pool (Pool)
├── async_node.rs    # Async connection (AsyncNode)
├── async_pool.rs    # Async connection pool (AsyncPool)
├── encode.rs        # Type marshalling (Value trait)
├── encode_option.rs # Option<T> encoding
├── table.rs         # VoltTable result handling
├── protocol.rs      # Wire protocol handling
├── response.rs      # Response parsing
├── error.rs         # Error types
└── pool_core.rs     # Shared pool abstractions
```

## Questions?

Feel free to open a GitHub Discussion or reach out to the maintainers.

Thank you for contributing!
