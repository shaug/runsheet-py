# Security Policy

## Reporting a vulnerability

If you discover a security vulnerability, please report it privately via [GitHub Security Advisories](https://github.com/shaug/runsheet-py/security/advisories/new).

Please do not open a public issue for security vulnerabilities.

## Scope

runsheet is an in-memory pipeline orchestration library. It does not handle network I/O, authentication, or data storage directly. Security concerns most relevant to this project include:

- Input validation bypass in schema handling
- Unexpected code execution via step composition
- Information leakage through error messages or context accumulation
