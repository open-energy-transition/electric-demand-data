<h1 align="center">Electricity Demand Data</h1>

<h3 align="center">
    <p>Supported by</p>
</h3>

<p align="center">
    <a href="https://www.breakthroughenergy.org/">
        <img src="docs/BE_logo.png" alt="Breakthrough Energy Logo" width="512"/>
    </a>
</p>

## About

Electric Demand Data is a Python-based project focused on collecting, processing, and forecasting electricity demand data. The aim of this project is to support energy planning studies by using machine learning models to generate time series of future electricity demand or for countries without available data.

### Features

- Retrieval of open hourly and sub-hourly electricity demand data from public sources ([ETL](https://github.com/open-energy-transition/electric-demand-data/tree/main/ETL)).
- Retrieval of weather and socio-economic data ([ETL](https://github.com/open-energy-transition/electric-demand-data/tree/main/ETL)).
- Forecasting using machine learning models ([models](https://github.com/open-energy-transition/electric-demand-data/tree/main/models/)).
- Modular design for adding new countries or data sources.
- Support for reproducible, containerized development.

The project is in active development, we are always looking for suggestions and contributions!

## Repository structure

```
electric-demand-data/
├── docs/                   # Project documentation (MkDocs)
├── ETL/                    # Scripts for extracting, transforming, and loading data
├── models/                 # Machine learning models for demand forecasting
│   └── xgboost/
├── .devcontainer/          # Development container configuration
├── .github/                # Github specifics such as actions
├── .gitignore              # File lists what git ignores
├── .pre-commit-config.yaml # Pre-commit configuration
├── .python-version         # Python version
├── CONTRIBUTING.md         # Guide to contributing
├── Dockerfile              # Docker setup for containerized runs
├── mkdocs.yml              # Documentation configuration file
├── pyproject.toml          # Project metadata and dependencies
├── ruff.toml               # Ruff configuration
└── uv.lock                 # Lockfile for project's dependencies
```

## Data collection progress

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="ETL/figures/available_entities_dark_mode.png">
  <source media="(prefers-color-scheme: light)" srcset="ETL/figures/available_entities_light_mode.png">
  <img alt="Countries and subdivisions with available electricity demand data" src="ETL/figures/available_entities.png">
</picture>

Find the code that we used to retrieve the data in their respective files inside the [ETL](https://github.com/open-energy-transition/electric-demand-data/tree/main/ETL) folder.

## Getting started

### 1. Clone the Repository

```bash
git clone https://github.com/open-energy-transition/electric-demand-data.git
cd electric-demand-data
```

### 2. Set Up Your Environment

This project uses [`uv`](https://github.com/astral-sh/uv) as a pacakge manager. It can be used within the provided Dockerfile or installed standalone (see [installing uv](https://docs.astral.sh/uv/getting-started/installation/))

```bash
uv sync
```

Alternatively, you may use `conda` to install the packages listed in `pyproject.toml`.

### 3. Run Scripts

Scripts can be run directly using:

```bash
uv run path/to/script.py
```

Jupyter notebooks ([details](https://docs.astral.sh/uv/guides/integration/jupyter/#using-jupyter-within-a-project)) can be launched with:

```bash
uv run --with jupyter jupyter lab --allow-root
```

## Development workflow

### Run tests and check test coverage

```bash
uv run pytest
uv run pytest --cov
```

### Pre-commit and lint code

```bash
uvx pre-commit
uvx ruff format
uvx mypy
```

## Documentation

The [documentation](https://open-energy-transition.github.io/electric-demand-data/) is currently hosted on GitHub pages connected to this repository. It is built with [mkdocs](https://github.com/squidfunk/mkdocs-material).

To run it locally:

```bash
uv run mkdocs serve
```

## Contributing

We welcome contributions in the form of:
- Country-specific ETL modules
- New or improved forecasting models
- Documentation and testing enhancements

Please follow the repository’s structure and submit your changes via pull request.

## License

This project is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0).
