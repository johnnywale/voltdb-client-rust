# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.2.x   | :white_check_mark: |
| 0.1.x   | :x:                |

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly:

1. **Do NOT** open a public GitHub issue for security vulnerabilities
2. Email the maintainer directly at johnnywalee@gmail.com
3. Include the following in your report:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

## Response Timeline

- **Acknowledgment**: Within 48 hours
- **Initial Assessment**: Within 1 week
- **Fix Timeline**: Depends on severity
  - Critical: Within 7 days
  - High: Within 30 days
  - Medium/Low: Next regular release

## Security Best Practices

When using this library:

1. **Keep dependencies updated**: Use `cargo update` regularly and monitor Dependabot alerts
2. **Use secure connections**: VoltDB supports TLS; configure it at the server level
3. **Credential management**: Never hardcode credentials; use environment variables or secret management
4. **Connection timeouts**: Always configure appropriate timeouts to prevent resource exhaustion

```rust
use voltdb_client_rust::{Opts, Pool};
use std::time::Duration;

let opts = Opts::builder()
    .host("localhost", 21212)
    .user(&std::env::var("VOLTDB_USER").unwrap())
    .password(&std::env::var("VOLTDB_PASS").unwrap())
    .connect_timeout(Duration::from_secs(10))
    .read_timeout(Duration::from_secs(30))
    .build()?;
```

## Dependency Auditing

This project uses:
- `cargo-deny` for license and vulnerability checking
- Dependabot for automated dependency updates
- Regular `cargo audit` runs in CI

To audit locally:
```bash
cargo install cargo-deny
cargo deny check
```
