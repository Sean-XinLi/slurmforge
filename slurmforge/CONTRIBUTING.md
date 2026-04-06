# Contributing

## Development Setup

Use Python 3.10 or newer.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install --no-build-isolation -e '.[dev]'
```

## Local Checks

Run the test suite before opening a pull request:

```bash
pytest -q
```

If you change CLI behavior, generated templates, or persisted run-record contracts, update the relevant docs and tests in the same change.

## Pull Requests

- Keep changes scoped to one behavior change or one cleanup.
- Add or update tests for behavior changes.
- Prefer preserving backward-compatible config and run-record semantics unless the PR explicitly updates the contract.
- Include a short explanation of user-visible impact in the pull request description.

## Reporting Bugs

Open an issue with:

- the command you ran
- the relevant config snippet
- the observed error output
- the expected behavior
- Python version and platform details
