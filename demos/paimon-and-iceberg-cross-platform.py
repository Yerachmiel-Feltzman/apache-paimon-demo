#!/usr/bin/env python3
"""
Cross-Catalog Demo - Paimon's Iceberg Compatibility
===================================================

This demo demonstrates Paimon's Iceberg compatibility feature:

When you create a Paimon table with 'metadata.iceberg.storage' = 'hadoop-catalog',
Paimon automatically writes Iceberg-compatible metadata alongside its native metadata.
This allows the SAME physical table to be queried through both:
- Paimon catalog (native Paimon queries)
- Iceberg catalog (standard Iceberg queries)

This provides:
1. Interoperability between Paimon and Iceberg ecosystems
2. Ability to use tools from both ecosystems on the same data
3. Migration path between formats
4. Unified data access layer

Reference: https://paimon.apache.org/docs/1.3/iceberg/append-table/
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError


# ============================================================================
# Helper Functions
# ============================================================================

def print_query(sql_query, description=None):
    """Print SQL query with optional description."""
    if description:
        print(f"\n   📝 {description}")
    print(f"   SQL: {sql_query}")


def print_result(expected=None, actual=None, success_msg=None, failure_msg=None):
    """Print expected vs actual results comparison."""
    if expected is not None and actual is not None:
        print(f"\n   📊 Result Analysis:")
        print(f"   • Expected: {expected}")
        print(f"   • Actual: {actual}")
        if expected == actual:
            print(f"   ✅ Match! {success_msg or 'Results match expected value'}")
        else:
            print(f"   ⚠️  Mismatch! {failure_msg or 'Results differ from expected'}")
    elif success_msg:
        print(f"\n   ✅ {success_msg}")


# ============================================================================
# Configuration & Setup
# ============================================================================

def find_jar_files(base_dir):
    """Find Paimon, Iceberg, and Paimon-Iceberg JAR files in the jars directory."""
    jars_dir = os.path.join(base_dir, "jars")
    paimon_jar = None
    iceberg_jar = None
    paimon_iceberg_jar = None
    
    for file in os.listdir(jars_dir):
        if "paimon-spark" in file and file.endswith('.jar'):
            paimon_jar = os.path.join(jars_dir, file)
        elif "iceberg-spark-runtime" in file and file.endswith('.jar'):
            iceberg_jar = os.path.join(jars_dir, file)
        elif "paimon-iceberg" in file and file.endswith('.jar'):
            paimon_iceberg_jar = os.path.join(jars_dir, file)
    
    if not paimon_jar or not iceberg_jar:
        raise FileNotFoundError("JAR files not found. Please run setup.sh first.")
    
    return paimon_jar, iceberg_jar, paimon_iceberg_jar


def create_spark_session(base_dir):
    """Create Spark session with both Paimon and Iceberg catalogs configured."""
    paimon_jar, iceberg_jar, paimon_iceberg_jar = find_jar_files(base_dir)
    
    paimon_warehouse = f"file://{base_dir}/warehouse/paimon"
    # Iceberg metadata is written to <paimon-warehouse>/iceberg when using 
    # 'metadata.iceberg.storage' = 'hadoop-catalog'
    iceberg_warehouse = f"file://{base_dir}/warehouse/paimon/iceberg"
    
    print("🔧 Configuring Spark with dual catalogs")
    print(f"   Paimon JAR: {os.path.basename(paimon_jar)}")
    print(f"   Iceberg JAR: {os.path.basename(iceberg_jar)}")
    if paimon_iceberg_jar:
        print(f"   Paimon-Iceberg JAR: {os.path.basename(paimon_iceberg_jar)} (REST Catalog support)")
    print(f"   Paimon Warehouse: {paimon_warehouse}")
    print(f"   Iceberg Warehouse: {iceberg_warehouse}")
    print("   Note: Paimon writes Iceberg metadata to <warehouse>/iceberg subdirectory")
    
    # Build JAR list - include paimon-iceberg if available
    jars_list = [paimon_jar, iceberg_jar]
    if paimon_iceberg_jar:
        jars_list.append(paimon_iceberg_jar)
    jars_string = ",".join(jars_list)
    
    spark = SparkSession.builder \
        .appName("Cross-Platform Demo") \
        .config("spark.jars", jars_string) \
        .config("spark.sql.catalog.paimon_catalog", "org.apache.paimon.spark.SparkCatalog") \
        .config("spark.sql.catalog.paimon_catalog.warehouse", paimon_warehouse) \
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
        .config("spark.sql.catalog.iceberg_catalog.warehouse", iceberg_warehouse) \
        .config("spark.sql.catalog.iceberg_catalog.cache-enabled", "false") \
        .config("spark.sql.extensions",
                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions,"
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    # Print Spark config values for user inspection
    print("\n🔧 SparkSession configured with the following settings:")
    for item in spark.sparkContext.getConf().getAll():
        print(f"   {item[0]} = {item[1]}")
    return spark


def create_spark_session_with_rest_catalog(base_dir, rest_catalog_name, rest_catalog_uri):
    """
    Create a Spark session with both Paimon and REST catalog configured via Spark config properties.
    
    Args:
        base_dir: Base directory for finding JARs
        rest_catalog_name: Name of the REST catalog (e.g., 'iceberg_rest_catalog')
        rest_catalog_uri: URI of the REST catalog server (e.g., 'http://localhost:8181')
    
    Returns:
        SparkSession configured with both Paimon and REST catalog
    """
    paimon_jar, iceberg_jar, paimon_iceberg_jar = find_jar_files(base_dir)
    
    paimon_warehouse = f"file://{base_dir}/warehouse/paimon"
    
    # Build JAR list - include paimon-iceberg if available
    jars_list = [paimon_jar, iceberg_jar]
    if paimon_iceberg_jar:
        jars_list.append(paimon_iceberg_jar)
    jars_string = ",".join(jars_list)
    
    print(f"\n🔧 Creating Spark session with Paimon and REST catalog:")
    print(f"   Paimon Warehouse: {paimon_warehouse}")
    print(f"   REST Catalog Name: {rest_catalog_name}")
    print(f"   REST Catalog URI: {rest_catalog_uri}")
    
    spark = SparkSession.builder \
        .appName("REST Catalog Demo") \
        .config("spark.jars", jars_string) \
        .config("spark.sql.catalog.paimon_catalog", "org.apache.paimon.spark.SparkCatalog") \
        .config("spark.sql.catalog.paimon_catalog.warehouse", paimon_warehouse) \
        .config(f"spark.sql.catalog.{rest_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{rest_catalog_name}.type", "rest") \
        .config(f"spark.sql.catalog.{rest_catalog_name}.uri", rest_catalog_uri) \
        .config("spark.sql.extensions",
                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions,"
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("FATAL")
    
    # Print all Spark config key/values for full visibility
    print("\n🔎 All Spark Configurations:")
    all_confs = spark.sparkContext.getConf().getAll()
    for key, value in sorted(all_confs):
        print(f"   {key} = {value}")

    return spark


# ============================================================================
# Demo 1: Basic Cross-Platform Query
# ============================================================================

def demo_basic_cross_platform(spark):
    """
    Demo: Create a Paimon table with Iceberg compatibility and query it via both catalogs.
    
    This demonstrates the core feature: the same physical table can be accessed
    through both Paimon and Iceberg catalogs.
    """
    print("\n" + "="*70)
    print("📋 DEMO 1: Basic Cross-Platform Query")
    print("="*70)
    print("Goal: Verify the same physical table can be queried through both Paimon and Iceberg catalogs")
    print("="*70)
    
    table_name = "paimon_catalog.`default`.cities"
    
    # Create table with Iceberg compatibility
    print("\n📝 Step 1: Creating Paimon table with Iceberg compatibility...")
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            country STRING,
            name STRING
        )
        TBLPROPERTIES (
            'metadata.iceberg.storage' = 'hadoop-catalog'
        )
    """
    print_query(create_sql.strip(), "Creating table with Iceberg compatibility")
    spark.sql(create_sql)
    print("   ✅ Table created")
    
    # Insert sample data
    print("\n📥 Step 2: Inserting sample data...")
    insert_sql = f"""
        INSERT INTO {table_name} VALUES 
            ('usa', 'new york'),
            ('germany', 'berlin'),
            ('usa', 'chicago'),
            ('germany', 'hamburg')
    """
    print_query(insert_sql.strip(), "Inserting 4 rows of sample data")
    spark.sql(insert_sql)
    print("   ✅ Data inserted")
    print("   Expected: 4 rows inserted")
    
    # Query via Paimon catalog
    print("\n🔍 Step 3: Querying via Paimon catalog (native)...")
    query_paimon = f"SELECT * FROM {table_name} WHERE country = 'germany'"
    print_query(query_paimon, "Querying German cities via Paimon catalog")
    print("   Expected: 2 rows (berlin, hamburg)")
    result_paimon = spark.sql(query_paimon)
    result_paimon.show()
    paimon_count = result_paimon.count()
    print_result(expected=2, actual=paimon_count, 
                success_msg="Found expected 2 German cities via Paimon catalog")
    
    # Query via Iceberg catalog (cross-platform)
    print("\n🔍 Step 4: Querying the SAME table via Iceberg catalog (cross-platform)...")
    query_iceberg = "SELECT * FROM iceberg_catalog.`default`.cities WHERE country = 'germany'"
    print_query(query_iceberg, "Querying German cities via Iceberg catalog")
    print("   Expected: 2 rows (berlin, hamburg) - same data as Paimon query")
    result_iceberg = spark.sql(query_iceberg)
    result_iceberg.show()
    iceberg_count = result_iceberg.count()
    print_result(expected=2, actual=iceberg_count,
                success_msg="Found expected 2 German cities via Iceberg catalog")
    
    print("\n✨ SUCCESS! The same physical table can be queried through both catalogs!")
    print_result(expected=paimon_count, actual=iceberg_count,
                success_msg="Both catalogs return the same data - cross-platform compatibility confirmed!")


# ============================================================================
# Demo 2: Drop Table Behavior Test
# ============================================================================

def demo_drop_table_behavior(spark):
    """
    Demo: Test what happens when you drop a Paimon table with Iceberg compatibility.
    
    Question: Does dropping the Paimon table also drop the Iceberg mirror table?
    """
    print("\n" + "="*70)
    print("🧪 DEMO 2: Drop Table Behavior Test")
    print("="*70)
    print("Goal: Determine if dropping a Paimon table also removes its Iceberg mirror")
    print("Expected: Iceberg table should be automatically removed (best case)")
    print("="*70)
    
    table_name = "paimon_catalog.`default`.test_drop"
    iceberg_table_name = "iceberg_catalog.`default`.test_drop"
    
    # Step 1: Clean up any existing tables
    print("\n🧹 Step 1: Cleaning up existing tables (if any)...")
    drop_sql = f"DROP TABLE IF EXISTS {iceberg_table_name}"
    print_query(drop_sql, "Dropping Iceberg table if exists")
    try:
        spark.sql(drop_sql)
    except Exception as e:
        print(f"   ⚠️  {str(e)}")
    drop_sql = f"DROP TABLE IF EXISTS {table_name}"
    print_query(drop_sql, "Dropping Paimon table if exists")
    spark.sql(drop_sql)
    print("   ✅ Cleanup complete")
    
    # Step 2: Create Paimon table with Iceberg compatibility
    print("\n📝 Step 2: Creating Paimon table with Iceberg compatibility...")
    create_sql = f"""
        CREATE TABLE {table_name} (
            id INT,
            name STRING
        )
        TBLPROPERTIES (
            'metadata.iceberg.storage' = 'hadoop-catalog'
        )
    """
    print_query(create_sql.strip(), "Creating table with Iceberg compatibility")
    spark.sql(create_sql)
    print("   ✅ Table created")
    
    # Step 3: Insert data
    print("\n📥 Step 3: Inserting data...")
    insert_sql = f"""
        INSERT INTO {table_name} VALUES 
            (1, 'Alice'),
            (2, 'Bob'),
            (3, 'Charlie')
    """
    print_query(insert_sql.strip(), "Inserting 3 rows")
    spark.sql(insert_sql)
    print("   ✅ Data inserted")
    print("   Expected: 3 rows inserted")
    
    # Step 4: Verify both catalogs can see the table
    print("\n🔍 Step 4: Verifying table is accessible via both catalogs...")
    query_paimon = f"SELECT COUNT(*) as count FROM {table_name}"
    print_query(query_paimon, "Counting rows via Paimon catalog")
    paimon_count = spark.sql(query_paimon).collect()[0]['count']
    print_result(expected=3, actual=paimon_count,
                success_msg=f"Paimon catalog sees {paimon_count} rows")
    
    query_iceberg = f"SELECT COUNT(*) as count FROM {iceberg_table_name}"
    print_query(query_iceberg, "Counting rows via Iceberg catalog")
    iceberg_count = spark.sql(query_iceberg).collect()[0]['count']
    print_result(expected=3, actual=iceberg_count,
                success_msg=f"Iceberg catalog sees {iceberg_count} rows")
    
    # Step 5: Drop the Paimon table
    print("\n🗑️  Step 5: Dropping the Paimon table...")
    drop_sql = f"DROP TABLE {table_name}"
    print_query(drop_sql, "Dropping Paimon table")
    spark.sql(drop_sql)
    print("   ✅ Paimon table dropped")
    
    # Step 6: Try to query via Iceberg catalog
    print("\n🔍 Step 6: Testing if Iceberg table still exists after Paimon drop...")
    query_test = f"SELECT * FROM {iceberg_table_name}"
    print_query(query_test, "Querying Iceberg table after Paimon drop")
    print("   Expected: Table should NOT exist (automatically removed)")
    print("   Alternative: Table exists but queries fail (inconsistent state)")
    
    try:
        result = spark.sql(query_test)
        result.show()
        count = result.count()
        print(f"\n   ⚠️  RESULT: Iceberg table STILL EXISTS with {count} rows!")
        print("   💡 CONCLUSION:")
        print("   • Dropping the Paimon table does NOT drop the Iceberg metadata")
        print("   • However, the data files were deleted, leading to potential errors")
        print("   • You need to manually drop the Iceberg table as well")
        print_result(expected="Table removed", actual="Table exists",
                    failure_msg="Iceberg metadata persists independently")
    except Exception as e:
        error_msg = str(e)
        if "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
            print(f"\n   ✅ RESULT: Iceberg table was automatically removed!")
            print("   💡 CONCLUSION:")
            print("   • Dropping the Paimon table also removes the Iceberg metadata")
            print("   • This is the expected behavior for mirrored tables")
            print_result(expected="Table removed", actual="Table removed",
                        success_msg="Iceberg metadata automatically cleaned up")
        else:
            print(f"\n   ⚠️  RESULT: Iceberg table exists but is broken!")
            print(f"   Error: {error_msg}")
            print("\n   💡 CONCLUSION:")
            print("   • The Iceberg metadata still exists")
            print("   • But the data files were deleted with the Paimon table")
            print("   • This creates an inconsistent state")
            print(f"   • Manual cleanup needed: DROP TABLE {iceberg_table_name}")
            print_result(expected="Table removed", actual="Table exists (broken)",
                        failure_msg="Metadata persists but data files are gone")
    
    # Step 7: Clean up Iceberg table if it still exists
    print("\n🧹 Step 7: Final cleanup...")
    cleanup_sql = f"DROP TABLE IF EXISTS {iceberg_table_name}"
    print_query(cleanup_sql, "Cleaning up Iceberg table")
    try:
        spark.sql(cleanup_sql)
        print("   ✅ Iceberg table cleaned up")
    except Exception as e:
        print(f"   ⚠️  {str(e)}")
        print("   ✅ No Iceberg table to clean up")


# ============================================================================
# Demo 3: ALTER TABLE Compatibility Test
# ============================================================================

def demo_alter_table_compatibility(spark):
    """
    Demo: Test if ALTER TABLE can add Iceberg compatibility to existing tables.
    
    This tests whether you can enable Iceberg compatibility on a table that was
    created without it initially.
    """
    print("\n" + "="*70)
    print("🧪 DEMO 3: ALTER TABLE Compatibility Test")
    print("="*70)
    print("Goal: Test if ALTER TABLE can add Iceberg compatibility to existing tables")
    print("Expected: Either all data accessible OR only new data accessible after ALTER")
    print("="*70)
    
    table_name = "paimon_catalog.`default`.cities2"
    iceberg_table_name = "iceberg_catalog.`default`.cities2"
    
    # Step 1: Clean up - drop BOTH catalogs to avoid stale metadata
    print("\n🧹 Step 1: Cleaning up (drop tables if they exist)...")
    drop_sql = f"DROP TABLE IF EXISTS {iceberg_table_name}"
    print_query(drop_sql, "Dropping Iceberg table if exists")
    try:
        spark.sql(drop_sql)
    except Exception as e:
        print(f"   ⚠️  {str(e)}")
    drop_sql = f"DROP TABLE IF EXISTS {table_name}"
    print_query(drop_sql, "Dropping Paimon table if exists")
    spark.sql(drop_sql)
    print("   ✅ Cleanup complete")
    
    # Step 2: Create table WITHOUT Iceberg compatibility
    print("\n📝 Step 2: Creating table WITHOUT Iceberg compatibility...")
    create_sql = f"""
        CREATE TABLE {table_name} (
            country STRING,
            name STRING
        )
    """
    print_query(create_sql.strip(), "Creating table without Iceberg compatibility")
    spark.sql(create_sql)
    print("   ✅ Table created (no Iceberg compatibility)")
    
    # Step 3: Insert initial data
    print("\n📥 Step 3: Inserting initial data (before ALTER)...")
    insert_sql = f"""
        INSERT INTO {table_name} VALUES 
            ('usa', 'new york'),
            ('germany', 'berlin'),
            ('usa', 'chicago'),
            ('germany', 'hamburg')
    """
    print_query(insert_sql.strip(), "Inserting 4 rows before ALTER")
    spark.sql(insert_sql)
    print("   ✅ Initial data inserted")
    print("   Expected: 4 rows inserted")
    
    query_before = f"SELECT * FROM {table_name}"
    print_query(query_before, "Verifying data via Paimon catalog")
    spark.sql(query_before).show()
    
    # Step 4: ALTER TABLE to add Iceberg compatibility
    print("\n🔧 Step 4: ALTER TABLE to add Iceberg compatibility property...")
    alter_sql = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('metadata.iceberg.storage' = 'hadoop-catalog')
    """
    print_query(alter_sql.strip(), "Adding Iceberg compatibility via ALTER TABLE")
    spark.sql(alter_sql)
    print("   ✅ Property added via ALTER TABLE")
    
    # Step 4.1: Query via Iceberg catalog
    print("\n🔍 Step 4.1: Querying via Iceberg catalog...")
    query_iceberg = """
        SELECT * FROM iceberg_catalog.`default`.cities2 
        ORDER BY country, name
    """
    print_query(query_iceberg.strip(), "Querying via Iceberg catalog")
    try:
        spark.sql(query_iceberg).show()
        print("   ✅ Query succeeded! Found rows")
    except Exception as e:
        print("   ⚠️  Could not query via Iceberg catalog:")
        print(f"   Error: {str(e)}")

    # Step 5: Insert new data after ALTER
    print("\n📥 Step 5: Inserting NEW data AFTER ALTER...")
    insert_new_sql = f"""
        INSERT INTO {table_name} VALUES 
            ('france', 'paris'),
            ('spain', 'madrid')
    """
    print_query(insert_new_sql.strip(), "Inserting 2 new rows after ALTER")
    spark.sql(insert_new_sql)
    print("   ✅ New data inserted")
    print("   Expected: 2 additional rows (total should be 6)")
    
    # Step 6: Try to query via Iceberg catalog
    print("\n🔍 Step 6: Querying via Iceberg catalog after ALTER...")
    query_iceberg = """
        SELECT * FROM iceberg_catalog.`default`.cities2 
        ORDER BY country, name
    """
    print_query(query_iceberg.strip(), "Querying via Iceberg catalog")
    print("   Expected: At least 2 rows (new data), ideally 6 rows (all data)")
    print("   Hypothesis: If ALTER works, we should see at least the NEW data")
    
    try:
        result = spark.sql(query_iceberg)
        result.show()
        row_count = result.count()
        print(f"\n   ✅ Query succeeded! Found {row_count} rows")
        
        # Analyze what data is visible
        print("\n   📊 Analyzing data visibility:")
        query_old = """
            SELECT * FROM iceberg_catalog.`default`.cities2 
            WHERE country IN ('usa', 'germany')
        """
        print_query(query_old.strip(), "Counting old data (usa, germany)")
        old_data = spark.sql(query_old).count()
        query_new = """
            SELECT * FROM iceberg_catalog.`default`.cities2 
            WHERE country IN ('france', 'spain')
        """
        print_query(query_new.strip(), "Counting new data (france, spain)")
        new_data = spark.sql(query_new).count()
        
        print(f"   • Old data (usa, germany): {old_data} rows")
        print(f"   • New data (france, spain): {new_data} rows")
        
        # Draw conclusions with expected vs actual
        print("\n   💡 CONCLUSION:")
        if new_data > 0 and old_data == 0:
            print("   ALTER TABLE enables Iceberg compatibility for FUTURE writes only!")
            print_result(expected="All 6 rows", actual=f"{new_data} rows (only new data)",
                        failure_msg="Old data not accessible via Iceberg")
            print("   • Old data (before ALTER) is NOT accessible via Iceberg")
            print("   • New data (after ALTER) IS accessible via Iceberg")
            print("   • Recommendation: Migrate old data if needed")
        elif new_data > 0 and old_data > 0:
            print("   ✅ ALTER TABLE WORKS! All data is accessible via Iceberg!")
            print_result(expected="All 6 rows", actual=f"{row_count} rows",
                        success_msg="Both old and new data accessible")
            print("   • Both old and new data became accessible after ALTER")
            print("   • Paimon generates Iceberg metadata on-demand or retroactively")
            print("\n   📝 Note: Documentation recommends setting property at CREATE TABLE time")
        else:
            print("   ALTER TABLE does NOT fully enable Iceberg compatibility")
            print_result(expected="At least 2 rows", actual=f"{row_count} rows",
                        failure_msg="ALTER TABLE did not enable Iceberg compatibility")
            
    except Exception as e:
        error_msg = str(e)
        print(f"\n   ❌ Query failed!")
        print(f"   Error: {error_msg}")
        
        print("\n   💡 CONCLUSION: ALTER TABLE behavior may be inconsistent")
        print_result(expected="Table queryable", actual="Query failed",
                    failure_msg="ALTER TABLE may not enable Iceberg compatibility")
        print("   • Metadata generation might be asynchronous or on-demand")
        print("   • Some metadata files might be missing")
        print("\n   📝 RECOMMENDATION:")
        print("   • Set 'metadata.iceberg.storage' at CREATE TABLE time (recommended)")
        print("   • This is the documented and reliable approach")
        print("   • If using ALTER, test thoroughly and allow time for metadata generation")


# ============================================================================
# Demo 4: REST Catalog Integration Test
# ============================================================================

def demo_rest_catalog_integration():
    """
    Demo: Test Paimon's REST Catalog integration.
    
    This uses 'metadata.iceberg.storage' = 'rest-catalog' which not only
    writes Iceberg metadata locally, but also registers the table in an
    Iceberg REST Iceberg catalog server.
    
    Requires: Iceberg REST Iceberg catalog server running (see docker-compose.yaml)
    
    Note: Creates a separate Spark session with REST catalog configuration
    using Spark config properties (not SQL CREATE CATALOG).
    """
    print("\n" + "="*70)
    print("🧪 DEMO 4: REST Catalog Integration Test")
    print("="*70)
    print("Testing Paimon's integration with Iceberg REST Catalog")
    
    # REST catalog configuration - matches docker-compose.yaml
    rest_catalog_uri = os.getenv('ICEBERG_REST_URI', 'http://localhost:8181')
    rest_catalog_name = "iceberg_rest_catalog"
    
    print(f"\n⚙️  Configuration:")
    print(f"   REST Catalog Name: {rest_catalog_name}")
    print(f"   REST Catalog URI: {rest_catalog_uri}")
    print(f"   Note: Requires Iceberg REST Catalog server running")
    print(f"   Start with: docker-compose up -d iceberg-rest-catalog")
    print(f"   Override URI with: ICEBERG_REST_URI environment variable")
    
    # Get base directory for creating new Spark session
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    table_name = "paimon_catalog.`default`.rest_test"
    iceberg_rest_table = f"{rest_catalog_name}.`default`.rest_test"
    

    spark = create_spark_session_with_rest_catalog(
        base_dir, 
        rest_catalog_name, 
        rest_catalog_uri
    )
    print("   ✅ Spark session created with both Paimon and REST catalog")
    
    # Step 1: Clean up
    print("\n🧹 Step 1: Cleaning up...")
    cleanup_sql = f"DROP TABLE IF EXISTS {table_name}"
    print_query(cleanup_sql, "Dropping table if exists")
    spark.sql(cleanup_sql)
    print("   ✅ Cleanup complete")
    
    # Step 2: Create table with REST catalog integration using original session
    print("\n📝 Step 2: Creating Paimon table with REST catalog integration...")
    print("   Using original Spark session with Paimon catalog")
    create_table_sql = f"""
        CREATE TABLE {table_name} (
            id INT,
            name STRING,
            department STRING
        )
        TBLPROPERTIES (
            'metadata.iceberg.storage' = 'rest-catalog',
            'metadata.iceberg.rest.uri' = '{rest_catalog_uri}',
            'metadata.iceberg.rest.warehouse' = 'paimon_warehouse',
            'metadata.iceberg.rest.clients' = '1'
        )
    """
    print_query(create_table_sql.strip(), "Creating table with REST catalog integration")
    try:
        spark.sql(create_table_sql)
        print("   ✅ Table created with REST catalog integration")
    except Exception as e:
        error_msg = str(e)
        print(f"   ❌ Failed to create table with REST catalog")
        print(f"   Error: {error_msg[:200]}...")
        print("\n   💡 REQUIREMENTS:")
        print("   • Iceberg REST Catalog server must be running")
        print(f"   • Start with: docker-compose up -d iceberg-rest-catalog")
        print(f"   • Default URI: {rest_catalog_uri}")
        print("   • Requires: paimon-iceberg-1.3.0.jar dependency")
        print("   • Requires: JDK 11+")
        print("\n   📝 Skipping REST Catalog demo - server not available")
        return
    
    # Step 3: Insert data using original session
    print("\n📥 Step 3: Inserting data via Paimon catalog...")
    insert_sql = f"""
        INSERT INTO {table_name} VALUES 
            (1, 'Alice', 'Engineering'),
            (2, 'Bob', 'Sales'),
            (3, 'Charlie', 'Marketing')
    """
    print_query(insert_sql.strip(), "Inserting 3 rows via Paimon catalog")
    spark.sql(insert_sql)
    print("   ✅ Data inserted")
    
    # Step 4: Query via Paimon catalog
    print("\n🔍 Step 4: Querying via Paimon catalog:")
    query_paimon = f"SELECT * FROM {table_name} ORDER BY id"
    print_query(query_paimon, "Querying via Paimon catalog")
    spark.sql(query_paimon).show()
    
    # Step 5: Query via Iceberg REST catalog
    print("\n🔍 Step 5: Querying via Iceberg REST catalog...")
    print(f"   Using: {iceberg_rest_table}")
    query_rest = f"SELECT * FROM {iceberg_rest_table} ORDER BY id"
    print_query(query_rest, "Querying via Iceberg REST catalog")
    try:
        result = spark.sql(query_rest)
        result.show()
        count = result.count()
        print(f"\n   ✅ SUCCESS! Table is accessible via Iceberg REST Catalog!")
        print(f"   Found {count} rows via REST catalog")
        print("\n   💡 This demonstrates:")
        print("   • Paimon wrote metadata to REST catalog server")
        print("   • The table is now registered in the REST catalog")
        print("   • Iceberg tools can discover and query this table via REST API")
        print("   • REST catalog configured via Spark config properties (not SQL)")
    except Exception as e:
        error_msg = str(e)
        print(f"\n   ❌ Failed to query via REST catalog")
        print(f"   Error: {error_msg[:200]}...")
        print("\n   💡 Possible issues:")
        print("   • REST catalog server might not be running")
        print("   • Table might not be registered yet")
        print("   • Network connectivity issue")
        print("   • Table name or namespace mismatch")
    
    # Step 6: Test the three commit scenarios
    print("\n🧪 Step 6: Testing REST Catalog commit behavior...")
    print("   According to docs, there are 3 cases:")
    print("   1. Table doesn't exist → creates it ✅ (already tested)")
    print("   2. Table exists and is compatible → commits metadata")
    print("   3. Table exists but is incompatible → drops and recreates")
    
    # Test case 2: Compatible update (using original session)
    print("\n   Testing case 2: Compatible update...")
    insert_update_sql = f"""
        INSERT INTO {table_name} VALUES 
            (4, 'Diana', 'HR')
    """
    print_query(insert_update_sql.strip(), "Inserting new row (compatible update)")
    spark.sql(insert_update_sql)
    print("   ✅ New data inserted (should commit to existing REST catalog table)")
    
    # Verify via REST catalog using REST session
    count_query = f"SELECT COUNT(*) as cnt FROM {iceberg_rest_table}"
    print_query(count_query, "Counting rows via REST catalog")
    try:
        count = spark.sql(count_query).collect()[0]['cnt']
        print(f"   ✅ REST catalog now shows {count} rows (expected: 4)")
        if count == 4:
            print("   ✅ Case 2 confirmed: Compatible updates work correctly!")
    except Exception as e:
        print(f"   ⚠️  Could not verify via REST catalog: {str(e)[:100]}")
    
    print("\n   💡 Case 3 (incompatible schema) would require ALTER TABLE")
    print("      This would trigger drop+recreate in REST catalog")
    
    # ========================================================================
    # Edge Case Tests
    # ========================================================================
    
    # Edge Case 1: Transform existing Paimon table to Iceberg REST catalog
    print("\n" + "="*70)
    print("🔬 EDGE CASE 1: Transform Existing Paimon Table to Iceberg REST Catalog")
    print("="*70)
    print("Testing: Can we add REST catalog compatibility to an existing table?")
    
    edge_case_table = "paimon_catalog.`default`.rest_alter_test"
    edge_case_rest_table = f"{rest_catalog_name}.`default`.rest_alter_test"
    
    # Clean up
    print("\n🧹 Cleaning up edge case test table...")
    drop_edge_sql = f"DROP TABLE IF EXISTS {edge_case_table}"
    print_query(drop_edge_sql, "Dropping edge case test table if exists")
    spark.sql(drop_edge_sql)
    print("   ✅ Cleanup complete")
    
    # Create table WITHOUT REST catalog compatibility
    print("\n📝 Creating Paimon table WITHOUT REST catalog compatibility...")
    create_edge_sql = f"""
        CREATE TABLE {edge_case_table} (
            id INT,
            name STRING,
            value INT
        )
    """
    print_query(create_edge_sql.strip(), "Creating table without REST catalog compatibility")
    spark.sql(create_edge_sql)
    print("   ✅ Table created (no REST catalog compatibility)")
    
    # Insert initial data
    print("\n📥 Inserting initial data (before ALTER)...")
    insert_edge_sql = f"""
        INSERT INTO {edge_case_table} VALUES 
            (1, 'Item A', 100),
            (2, 'Item B', 200),
            (3, 'Item C', 300)
    """
    print_query(insert_edge_sql.strip(), "Inserting 3 initial rows")
    spark.sql(insert_edge_sql)
    print("   ✅ Initial data inserted")
    print("\n   📊 Data visible via Paimon catalog:")
    query_edge = f"SELECT * FROM {edge_case_table}"
    print_query(query_edge, "Querying data via Paimon catalog")
    spark.sql(query_edge).show()
    
    # ALTER TABLE to add REST catalog compatibility
    print("\n🔧 ALTER TABLE to add REST catalog compatibility property...")
    alter_edge_sql = f"""
        ALTER TABLE {edge_case_table}
        SET TBLPROPERTIES (
            'metadata.iceberg.storage' = 'rest-catalog',
            'metadata.iceberg.rest.uri' = '{rest_catalog_uri}',
            'metadata.iceberg.rest.warehouse' = 'paimon_warehouse',
            'metadata.iceberg.rest.clients' = '1'
        )
    """
    print_query(alter_edge_sql.strip(), "Adding REST catalog compatibility via ALTER TABLE")
    try:
        spark.sql(alter_edge_sql)
        print("   ✅ Property added via ALTER TABLE")
    except Exception as e:
        error_msg = str(e)
        print(f"   ❌ Failed to ALTER TABLE")
        print(f"   Error: {error_msg}")
        print("\n   💡 CONCLUSION: ALTER TABLE may not support REST catalog")
        print("   • REST catalog integration might require CREATE TABLE time configuration")
        drop_edge_failed_sql = f"DROP TABLE IF EXISTS {edge_case_table}"
        print_query(drop_edge_failed_sql, "Cleaning up test table after failed ALTER")
        spark.sql(drop_edge_failed_sql)
        print("   ✅ Cleaned up test table")
    else:
        print("\n🔍 Querying table via REST catalog BEFORE adding new data (sanity check)...")
        query_before_alter = f"SELECT * FROM {edge_case_rest_table} ORDER BY id"
        print_query(query_before_alter, "Querying via REST catalog before new data")
        try:
            spark.sql(query_before_alter).show()
        except Exception as e:
            print("   ⚠️  Could not query via REST catalog before new data:")
            print(f"   Error: {str(e)[:200]}...")

        # Insert new data after ALTER
        print("\n📥 Inserting NEW data AFTER ALTER...")
        insert_new_edge_sql = f"""
            INSERT INTO {edge_case_table} VALUES 
                (4, 'Item D', 400),
                (5, 'Item E', 500)
        """
        print_query(insert_new_edge_sql.strip(), "Inserting 2 new rows after ALTER")
        spark.sql(insert_new_edge_sql)
        print("   ✅ New data inserted")
        
        # Try to query via REST catalog
        print("\n🔍 Testing if table is accessible via REST catalog after ALTER...")
        query_after_alter = f"SELECT * FROM {edge_case_rest_table} ORDER BY id"
        print_query(query_after_alter, "Querying via REST catalog after ALTER")
        try:
            result = spark.sql(query_after_alter)
            result.show()
            count = result.count()
            print(f"\n   ✅ Query succeeded! Found {count} rows")
            
            # Analyze what data is visible
            print("\n   📊 Analyzing data visibility:")
            query_old_edge = f"""
                SELECT * FROM {edge_case_rest_table} 
                WHERE id IN (1, 2, 3)
            """
            print_query(query_old_edge.strip(), "Counting old data (id 1-3)")
            old_data = spark.sql(query_old_edge).count()
            query_new_edge = f"""
                SELECT * FROM {edge_case_rest_table} 
                WHERE id IN (4, 5)
            """
            print_query(query_new_edge.strip(), "Counting new data (id 4-5)")
            new_data = spark.sql(query_new_edge).count()
            
            print(f"   • Old data (id 1-3): {old_data} rows")
            print(f"   • New data (id 4-5): {new_data} rows")
            
            print("\n   💡 CONCLUSION:")
            if new_data > 0 and old_data == 0:
                print("   ALTER TABLE enables REST catalog for FUTURE writes only!")
                print("   • Old data (before ALTER) is NOT accessible via REST catalog")
                print("   • New data (after ALTER) IS accessible via REST catalog")
            elif new_data > 0 and old_data > 0:
                print("   ✅ ALTER TABLE WORKS! All data is accessible via REST catalog!")
                print("   • Both old and new data became accessible after ALTER")
            else:
                print("   ALTER TABLE does NOT fully enable REST catalog compatibility")
        except Exception as e:
            error_msg = str(e)
            print(f"\n   ❌ Query failed!")
            print(f"   Error: {error_msg}")
            print("\n   💡 CONCLUSION: ALTER TABLE may not enable REST catalog compatibility")
            print("   • REST catalog integration likely requires CREATE TABLE configuration")
        
        # Cleanup edge case table
        print("\n🧹 Cleaning up edge case test table...")
        try:
            drop_rest_edge_sql = f"DROP TABLE IF EXISTS {edge_case_rest_table}"
            print_query(drop_rest_edge_sql, "Dropping REST catalog edge case table")
            spark.sql(drop_rest_edge_sql)
        except:
            pass
        drop_edge_sql = f"DROP TABLE IF EXISTS {edge_case_table}"
        print_query(drop_edge_sql, "Dropping Paimon edge case table")
        spark.sql(drop_edge_sql)
        print("   ✅ Edge case 1 cleanup complete")
    
    # Edge Case 2: Drop Paimon table and check REST catalog
    print("\n" + "="*70)
    print("🔬 EDGE CASE 2: Drop Paimon Table and Check REST Catalog")
    print("="*70)
    print("Testing: Does dropping a Paimon table also remove it from REST catalog?")
    
    drop_test_table = "paimon_catalog.`default`.rest_drop_test"
    drop_test_rest_table = f"{rest_catalog_name}.`default`.rest_drop_test"
    
    # Clean up
    print("\n🧹 Cleaning up drop test table...")
    try:
        drop_rest_test_sql = f"DROP TABLE IF EXISTS {drop_test_rest_table}"
        print_query(drop_rest_test_sql, "Dropping REST catalog drop test table if exists")
        spark.sql(drop_rest_test_sql)
    except:
        pass
    drop_test_sql = f"DROP TABLE IF EXISTS {drop_test_table}"
    print_query(drop_test_sql, "Dropping Paimon drop test table if exists")
    spark.sql(drop_test_sql)
    print("   ✅ Cleanup complete")
    
    # Create table WITH REST catalog compatibility
    print("\n📝 Creating Paimon table WITH REST catalog compatibility...")
    create_drop_test_sql = f"""
        CREATE TABLE {drop_test_table} (
            id INT,
            name STRING
        )
        TBLPROPERTIES (
            'metadata.iceberg.storage' = 'rest-catalog',
            'metadata.iceberg.rest.uri' = '{rest_catalog_uri}',
            'metadata.iceberg.rest.warehouse' = 'paimon_warehouse',
            'metadata.iceberg.rest.clients' = '1'
        )
    """
    print_query(create_drop_test_sql.strip(), "Creating table with REST catalog compatibility")
    spark.sql(create_drop_test_sql)
    print("   ✅ Table created with REST catalog compatibility")
    
    # Insert data
    print("\n📥 Inserting data...")
    insert_drop_test_sql = f"""
        INSERT INTO {drop_test_table} VALUES 
            (1, 'Test A'),
            (2, 'Test B'),
            (3, 'Test C')
    """
    print_query(insert_drop_test_sql.strip(), "Inserting 3 test rows")
    spark.sql(insert_drop_test_sql)
    print("   ✅ Data inserted")
    
    # Verify both catalogs can see the table
    print("\n🔍 Verifying table is accessible via both catalogs...")
    print("   Via Paimon:")
    count_paimon_drop = f"SELECT COUNT(*) as count FROM {drop_test_table}"
    print_query(count_paimon_drop, "Counting rows via Paimon catalog")
    paimon_count = spark.sql(count_paimon_drop).collect()[0]['count']
    print(f"   ✅ Paimon catalog sees {paimon_count} rows")
    
    print("   Via REST catalog:")
    count_rest_drop = f"SELECT COUNT(*) as count FROM {drop_test_rest_table}"
    print_query(count_rest_drop, "Counting rows via REST catalog")
    try:
        rest_count = spark.sql(count_rest_drop).collect()[0]['count']
        print(f"   ✅ REST catalog sees {rest_count} rows")
    except Exception as e:
        print(f"   ⚠️  REST catalog query failed: {str(e)[:100]}...")
        print("   Note: Table might not be registered yet")
        rest_count = 0
    
    # Drop the Paimon table
    print("\n🗑️  Dropping the Paimon table...")
    drop_paimon_sql = f"DROP TABLE {drop_test_table}"
    print_query(drop_paimon_sql, "Dropping Paimon table")
    spark.sql(drop_paimon_sql)
    print("   ✅ Paimon table dropped")
    
    # Try to query via REST catalog
    print("\n🔍 Testing if REST catalog table still exists...")
    query_drop_test = f"SELECT * FROM {drop_test_rest_table}"
    print_query(query_drop_test, "Querying REST catalog table after Paimon drop")
    try:
        result = spark.sql(query_drop_test)
        result.show()
        count = result.count()
        print(f"\n   ⚠️  RESULT: REST catalog table STILL EXISTS with {count} rows!")
        print("   💡 CONCLUSION:")
        print("   • Dropping the Paimon table does NOT remove it from REST catalog")
        print("   • REST catalog metadata persists independently")
        print("   • However, data files were deleted, so queries may fail")
        print("   • You need to manually drop the REST catalog table")
    except Exception as e:
        error_msg = str(e)
        print(error_msg)
        if "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
            print(f"\n   ✅ RESULT: REST catalog table was automatically removed!")
            print("   💡 CONCLUSION:")
            print("   • Dropping the Paimon table also removes it from REST catalog")
            print("   • This is the expected behavior for mirrored tables")
        else:
            print(f"\n   ⚠️  RESULT: REST catalog table exists but is broken!")
            print(f"   Error: {error_msg[:150]}...")
            print("\n   💡 CONCLUSION:")
            print("   • The REST catalog metadata still exists")
            print("   • But the data files were deleted with the Paimon table")
            print("   • This creates an inconsistent state")
            print(f"   • You should manually clean up: DROP TABLE {drop_test_rest_table}")
    
    # Cleanup REST catalog table if it still exists
    print("\n🧹 Final cleanup...")
    try:
        drop_final_rest_sql = f"DROP TABLE IF EXISTS {drop_test_rest_table}"
        print_query(drop_final_rest_sql, "Dropping REST catalog table if exists")
        spark.sql(drop_final_rest_sql)
        print("   ✅ REST catalog table cleaned up")
    except:
        print("   ✅ No REST catalog table to clean up")
    
    # Cleanup main demo table
    print("\n🧹 Cleaning up main demo table...")
    try:
        drop_main_rest_sql = f"DROP TABLE IF EXISTS {iceberg_rest_table}"
        print_query(drop_main_rest_sql, "Dropping main REST catalog table if exists")
        spark.sql(drop_main_rest_sql)
    except:
        pass
    drop_main_sql = f"DROP TABLE IF EXISTS {table_name}"
    print_query(drop_main_sql, "Dropping main Paimon table if exists")
    spark.sql(drop_main_sql)
    print("   ✅ Cleanup complete")
    
    # Stop REST catalog session
    if spark is not None:
        spark.stop()
        print("   ✅ REST catalog Spark session stopped")


# ============================================================================
# Main Demo Orchestration
# ============================================================================

def run_all_demos(spark):
    """Run all demo scenarios."""
    demo_basic_cross_platform(spark)
    demo_drop_table_behavior(spark)
    demo_alter_table_compatibility(spark)
    demo_rest_catalog_integration()  # requires REST Iceberg catalog server


def main():
    """Main entry point for the demo."""
    print("🚀 Cross-Platform Demo: Paimon + Iceberg")
    print("=" * 70)
    print("Demonstrating Paimon's Iceberg compatibility feature")
    print("=" * 70)
    
    # Get base directory
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    spark = None
    try:
        # Setup Spark session
        spark = create_spark_session(base_dir)
        
        # Run demos
        run_all_demos(spark)
        
        print("\n" + "="*70)
        print("✅ All demos completed successfully!")
        print("="*70)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Demo failed: {str(e)}")
        return 1
        
    finally:
        if spark is not None:
            spark.stop()
            print("\n🛑 Spark session stopped")


if __name__ == "__main__":
    sys.exit(main())
