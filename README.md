# Setup

## Prerequisites
- Java 17+ (I recommend using SDKMAN to install Java so it will pick up the correct version automatically from the [.sdkmanrc](.sdkmanrc) file.)
- Python 3.11 (I recommend using pyenv to install Python so it will pick up the correct version automatically from the [.python-version](.python-version) file.)

## Installation
Run the setup script. It will:
1. Create the necessary directories
2. Download the necessary JARs:
   - `paimon-spark-3.4-1.3.0.jar` - Paimon Spark integration
   - `iceberg-spark-runtime-3.4_2.12-1.10.0.jar` - Iceberg Spark runtime
   - `paimon-iceberg-1.3.0.jar` - Paimon-Iceberg compatibility (REST Catalog support)
3. Create a Python virtual environment
4. Install the necessary Python dependencies
5. Create sample data

```bash
chmod +x setup.sh
./setup.sh
```

# Paimon-only Demo

```bash
make run_paimon_only_demo
```
💡 Key Takeaways:
- Paimon provides ACID transactions with primary keys
- Write operations (UPSERT tested) work with Spark SQL
- Read operations work with Spark SQL

# Paimon + Iceberg Cross-Platform Demo

```bash
make run_paimon_and_iceberg_cross_platform_demo
```

This demo tests Paimon's Iceberg compatibility feature with multiple scenarios:

## Demo 1: Basic Cross-Platform Query
Tests the core feature - querying the same physical table through both Paimon and Iceberg catalogs.
- Creates a Paimon table with `'metadata.iceberg.storage' = 'hadoop-catalog'`
- Writes data via Paimon catalog
- Queries data via both Paimon and Iceberg catalogs
- **Result**: ✅ Same physical data accessible through both catalogs. Both catalogs return identical results, confirming cross-platform compatibility works as expected.

## Demo 2: Drop Table Behavior
Tests what happens when you drop a Paimon table with Iceberg compatibility.
- Creates a table with dual-catalog support
- Verifies it's accessible via both catalogs
- Drops the Paimon table
- Tests if Iceberg metadata is automatically cleaned up
- **Result**: ⚠️ **Inconsistent state**: Dropping the Paimon table deletes physical files, but Iceberg metadata still references them. Querying via Iceberg catalog after drop results in `FileNotFoundException`. Iceberg metadata is not automatically cleaned up when Paimon table is dropped.

## Demo 3: ALTER TABLE Compatibility
Tests if you can add Iceberg compatibility to an existing table.
- Creates a table WITHOUT Iceberg compatibility
- Inserts data
- Uses ALTER TABLE to add `'metadata.iceberg.storage'`
- Inserts new data after ALTER
- Tests if the table becomes queryable via Iceberg
- **Result**: ✅ **ALTER TABLE works with lazy metadata generation**: After ALTER TABLE, the table is not immediately queryable via Iceberg. However, after inserting new data, Iceberg metadata is generated retroactively and ALL data (both old and new) becomes accessible via Iceberg catalog. This demonstrates that Paimon generates Iceberg metadata on-demand during write operations.

## Demo 4: REST Catalog Integration
Tests Paimon's integration with Iceberg REST Catalog servers.
- Requires: Iceberg REST Catalog server running (`docker-compose up -d iceberg-rest-catalog`)
- Uses `'metadata.iceberg.storage' = 'rest-catalog'` with REST catalog properties
- Registers table in REST catalog (not just files)
- **Result**: ✅ **Works when configured at CREATE TABLE time**: Tables created with REST catalog integration are successfully registered and queryable via REST catalog. However, ALTER TABLE does NOT enable REST catalog compatibility - it must be configured during CREATE TABLE. Drop behavior is similar to Demo 2 (inconsistent state).

# Clean up
```bash
chmod +x cleanup.sh
./cleanup.sh
```