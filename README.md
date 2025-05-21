<h1 align="center">Electricity Demand Data</h1>

<h3 align="center">
    Global hourly electricity demand forecasting
</h3>

<p align="center">
    <a href="https://open-energy-transition.github.io/electric-demand-data/">
        <b>Documentation</b>
    </a>
</p>

<h3 align="center">
    <b>Supported by</b>
</h3>

<p align="center">
    <a href="https://www.breakthroughenergy.org/">
        <img src="docs/BE_logo.png" alt="Breakthrough Energy Logo" width="512"/>
    </a>
</p>

## About

This project is aimed at improving the demand forecasts that are used in energy planning studies.

It includes the process to obtain the open data from various sources (see [ETL](https://github.com/open-energy-transition/electric-demand-data/tree/main/ETL)) and the models that use the data to predict electricity demand (see [models](https://github.com/open-energy-transition/electric-demand-data/tree/main/models/)).

The project is in active development, we are always looking for suggestions and contributions!

## Data collection progress

![Data availability](ETL/figures/available_countries.png "Countries and subdivisions with available electricity demand data")
Find the code that we used to retrieve the data in their respective files inside the [ETL](https://github.com/open-energy-transition/electric-demand-data/tree/main/ETL) folder.

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

The [documentation](https://open-energy-transition.github.io/electric-demand-data/) is currently hosted on GitHub pages connected to this repository.
It is built with [mkdocs](https://github.com/squidfunk/mkdocs-material).
To run it locally:

```bash
uv run mkdocs serve
```
