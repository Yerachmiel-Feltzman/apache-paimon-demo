# Agent Instructions

## Setup
Run `make setup` before any demo. It downloads JARs, creates `.venv`, generates sample data, and starts the Iceberg REST Catalog Docker container.

## Prerequisites
- Java 17+ (auto-selected via `.sdkmanrc` if using SDKMAN)
- Python 3.11 (auto-selected via `.python-version` if using pyenv)
- Docker (required for REST Catalog integration in cross-platform demo)

## Run Commands
- `make run_paimon_only_demo` — basic Paimon ACID operations (Python script)
- `make run_paimon_and_iceberg_cross_platform_demo` — Paimon ↔ Iceberg cross-catalog queries (Python script)
- `make run_paimon_only_notebook` — basic Paimon ACID operations (Jupyter notebook)
- `make run_paimon_and_iceberg_cross_platform_notebook` — Paimon ↔ Iceberg cross-catalog queries (Jupyter notebook)

## Version Constraints
Spark, Scala, Paimon, and Iceberg versions are pinned in `setup.sh`. If updating, all three JARs must remain compatible (see `find_jar_files()` in demos for expected naming patterns).

## Virtual Environment
Scripts assume `.venv` is active. Activate with `source .venv/bin/activate` if running Python files directly instead of via `make`.

## Cleanup
`make cleanup` removes `jars/`, `warehouse/`, `data/` and stops the Docker container.
