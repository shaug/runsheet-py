# Contributing to runsheet

## Development setup

```bash
git clone https://github.com/shaug/runsheet-py.git
cd runsheet-py
uv sync --dev
uv run pre-commit install --hook-type pre-commit --hook-type commit-msg
```

## Running checks

```bash
uv run pytest                      # tests
uv run pytest --cov                # tests with coverage
uv run pyright                     # type checking
uv run ruff check .                # linting
uv run ruff format --check .       # format check
```

Or let pre-commit run everything on commit.

## Commit conventions

This project uses [Conventional Commits](https://www.conventionalcommits.org/). The pre-commit hook enforces this.

```
feat: add new combinator
fix: correct filter_step predicate signature
docs: update README examples
test: add coverage for retry backoff
refactor: simplify middleware composition
chore: update dependencies
```

These prefixes drive automated changelog generation and semantic versioning via release-please.

## Pull requests

1. Fork the repo and create a branch from `main`
2. Make your changes
3. Ensure all checks pass: `uv run pytest && uv run pyright && uv run ruff check .`
4. Open a PR against `main`

## Releases

Releases are automated via [release-please](https://github.com/googleapis/release-please). When conventional commits land on `main`, release-please maintains a PR with the version bump and changelog. Merging that PR creates a GitHub release, which triggers publishing to PyPI.
