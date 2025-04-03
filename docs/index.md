# Welcome



For full documentation visit [mkdocs.org](https://www.mkdocs.org).

## Commands

* `mkdocs new [dir-name]` - Create a new project.
* `mkdocs serve` - Start the live-reloading docs server.
* `mkdocs build` - Build the documentation site.
* `mkdocs -h` - Print help message and exit.

## Project layout

### Folders

    ETL/                    # Extract Transform Load
    docs/                   # Documentation
    models/                 # Contains code for our models

### Files

    .devcontainer           # Related to VS Code for running containers
	.github                 # Github specifics such as actions
    .gitignore              # File lists what git ignores
    .pre-commit-config.yaml # Pre-commit configuration
    .python-version
    CONTRIBUTING.md         # Guide to contributing
    Dockerfile              # Instructions how to build container
    LICENSE
    README.md
    mkdocs.yml              # Documentation configuration file.
    pyproject.toml          # Project dependencies
    ruff.toml               # Ruff configuration
    uv.lock
