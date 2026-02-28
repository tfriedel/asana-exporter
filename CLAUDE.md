# Development Guidelines

## Project Overview

CLI tool to extract data from Asana workspaces. Exports projects, tasks, subtasks, stories, and attachments as JSON, with optional SQLite/FTS5 import for full-text search.

## Architecture

```
src/asana_exporter/
├── client.py       # Main CLI, API resource classes, extractor orchestration
├── utils.py        # Logger, decorators (with_lock, required)
└── database/
    ├── schema.py   # SQLite schema creation with FTS5 virtual tables
    └── loader.py   # JSON-to-SQLite importer with incremental sync
```

## Quick Start

```bash
just setup           # Install deps and pre-commit hooks
uv run asana-exporter --help
just test            # Run tests
just lint            # Check code quality
just format          # Format code
just ci              # Full CI pipeline
```

## Git & PRs

- Always create PRs against `tfriedel/asana-exporter`, never against the upstream `dosaboy/asana-exporter`.

## Package Management

- ONLY use uv, NEVER pip
- Installation: `uv add package`
- Upgrading: `uv add --dev package --upgrade-package package`
- FORBIDDEN: `uv pip install`, `@latest` syntax

## Code Quality Standards

- Type hints required for all code
- Follow existing patterns exactly
- Use Google style for docstrings
- Ruff enforces: ANN, B, D, E, F, I, PTH, RUF, SIM, UP, W
- Use `pathlib.Path` instead of `os.path` (PTH rule)
- Use f-strings instead of %-formatting (UP rule)
- Use `raise ValueError`/`RuntimeError` instead of `assert` for validation

## Test-Driven Development (TDD)

### The TDD Mindset

- **Red-Green-Refactor cycle**: Write failing test → Make it pass → Improve the code
- **Tests define behavior**: Each test documents a specific business requirement
- **Design emergence**: Let the tests guide you to discover the right abstractions
- **Refactor when valuable**: Actively and frequently look for opportunities to make meaningful refactorings

### Critical TDD Rules

- **ONE TEST AT A TIME**: Add only a single test, see it fail (RED), implement minimal code to pass (GREEN), refactor (REFACTOR), repeat
- **MINIMAL IMPLEMENTATION**: Fix only the immediate test failure - do not implement complete functionality until tests demand it
- **NO BULK TEST ADDITION**: Never add multiple tests simultaneously - TDD Guard will block this
- **FAIL FIRST**: Always run the new test to confirm it fails before writing implementation code
- **INCREMENTAL PROGRESS**: Each test should drive one small increment of functionality

### Refactoring Triggers

After each green test, look for:

- **Duplication to extract**: Shared logic that can be centralized
- **Complex expressions to simplify**: Break down complicated logic into clear steps
- **Emerging patterns**: Abstractions suggested by repeated structures
- **Better names**: Clarify intent through descriptive naming
- **Code smells to eliminate**:
  - Logic crammed together without clear purpose
  - Mixed concerns (business logic, calculations, data handling in one place)
  - Hard-coded values that should be configurable
  - Similar operations repeated inline instead of abstracted
  - High coupling between components
  - Poor extensibility requiring core logic changes

### Testing Best Practices

- Framework: `uv run --frozen pytest`
- **Shared code reuse**: Import shared logic from production code, never duplicate in tests
- **Test data factories**: Create functions that generate test data with sensible defaults
- **Business-focused tests**: Test names describe business value, not technical details
- Coverage: test edge cases and errors
- New features require tests
- Bug fixes require regression tests

### Common TDD Violations to Avoid

- Adding 4+ tests at once (blocked by TDD Guard)
- Over-implementing when test only needs imports or basic structure
- Writing implementation code before seeing test fail
- Implementing features not yet demanded by tests

### TDD Development Cycle

1. Write ONE test that fails (RED)
2. Run test to confirm failure: `uv run --frozen pytest path/to/test.py::test_name -v`
3. Write minimal code to make test pass (GREEN)
4. Run test to confirm it passes
5. Refactor if needed while keeping tests green (REFACTOR)
6. Run `just lint` to catch issues like unused imports
7. Repeat with next single test

## Version Control

- Conventional Commits style for commit messages
- Work on feature branches
- Never commit secrets (API tokens, .env files)

## Code Formatting and Linting

```bash
just format      # Format with ruff
just lint        # Check with ruff
just lint-fix    # Auto-fix issues
just pre-commit  # Run pre-commit hooks
```

## Pre-commit Hooks

- Uses `prek` (fast Rust-based pre-commit runner)
- `sync-with-uv` keeps `.pre-commit-config.yaml` versions in sync with `uv.lock`
- Config: `.pre-commit-config.yaml`
- Tools: sync-with-uv, uv-lock, Ruff, Zuban
