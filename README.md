# Electricity Demand data

This repository will host the code related to the demand project.

## Development

This project uses [uv](https://github.com/astral-sh/uv) as a pacakge manager.
It can be used within the provided Dockerfile or installed standalone, see:
[Installing uv](https://docs.astral.sh/uv/getting-started/installation/)

Jupyter [(details)](https://docs.astral.sh/uv/guides/integration/jupyter/#using-jupyter-within-a-project):
```bash
uv run --with jupyter jupyter lab --allow-root
```

Testing:
```bash
uv run pytest
```

Run this before commits:
```bash
uvx pre-commit
```

Only run ruff:
```bash
uvx ruff check
# automatically fix issues
uvx ruff check --fix
```

Check the code coverage:
```bash
uv run pytest --cov
```

## Documentation

The documentation uses [mkdocs](https://github.com/squidfunk/mkdocs-material).
To run it locally:
```bash
uv run mkdocs serve
```
