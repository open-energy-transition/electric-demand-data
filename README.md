# Electricity Demand Data

This project is aimed at improving the demand forecasts that are used in energy planning studies.
It includes the process to obtain the open data from various sources (see ETL) and the models that use the data to predict electricity demand (see models).
The project is in active development, we are always looking for suggestions and contributions!


## Countries and regions for which retreival scripts of electricity demand data are available

![Data availability](https://github.com/open-energy-transition/electric-demand-data/tree/main/ETL/figures/available_countries.png "Countries and regions with available electricity demand data")

## Development

This project uses [uv](https://github.com/astral-sh/uv) as a pacakge manager.
It can be used within the provided Dockerfile or installed standalone, see:
[Installing uv](https://docs.astral.sh/uv/getting-started/installation/)

Then clone the repository, and run:
```bash
git clone https://github.com/open-energy-transition/electric-demand-data.git
uv sync
```

### Useful commands

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
