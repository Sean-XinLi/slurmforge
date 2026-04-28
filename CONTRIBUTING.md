# Contributing

## Development Setup

Use Python 3.10 or newer.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

## Local Checks

Run the test suite before opening a pull request:

```bash
ruff check src tests
pytest -q
```

If you change CLI behavior, generated templates, or persisted run-record contracts, update the relevant docs and tests in the same change.

## Pull Requests

- Keep changes scoped to one behavior change or one cleanup.
- Add or update tests for behavior changes.
- Treat config and run-record contract changes as explicit user-visible behavior changes.
- Include a short explanation of user-visible impact in the pull request description.

## Reporting Bugs

Open an issue with:

- the command you ran
- the relevant config snippet
- the observed error output
- the expected behavior
- Python version and platform details
