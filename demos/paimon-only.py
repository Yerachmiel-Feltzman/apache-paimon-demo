#!/usr/bin/env python3
"""
Paimon Demo - Learn Apache Paimon with Spark
============================================

This demo shows how to:
1. Create Paimon tables with primary keys
2. Insert and update data (ACID operations)
3. Query Paimon tables
4. Use Paimon's streaming capabilities
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

def create_spark_session():
    """Create Spark session configured for Paimon"""
    
    # Get JAR path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    paimon_jar = None
    
    jars_dir = os.path.join(base_dir, "jars")
    for file in os.listdir(jars_dir):
        if "paimon-spark" in file and file.endswith('.jar'):
            paimon_jar = os.path.join(jars_dir, file)
            break
    
    if not paimon_jar:
        raise FileNotFoundError("Paimon JAR not found. Please run setup.sh first.")
    
    warehouse_path = f"file://{base_dir}/warehouse/paimon"
    
    print(f"🔧 Configuring Spark with Paimon")
    print(f"   JAR: {os.path.basename(paimon_jar)}")
    print(f"   Warehouse: {warehouse_path}")
    
    spark = SparkSession.builder \
        .appName("Paimon Demo") \
        .config("spark.jars", paimon_jar) \
        .config("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions") \
        .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog") \
        .config("spark.sql.catalog.paimon.warehouse", warehouse_path) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def demo_paimon_basics(spark):
    """Demonstrate basic Paimon operations"""
    
    print("\n" + "="*70)
    print("📊 DEMO 1: Creating Paimon Tables")
    print("="*70)
    print("📚 Key Concept: Paimon tables use primary keys for ACID operations")
    print("="*70)
    
    # Create database
    print("\n📝 Step 1: Create a database in the Paimon catalog")
    print("-" * 50)
    sql = "CREATE DATABASE IF NOT EXISTS paimon.demo"
    print(f"SQL> {sql}")
    spark.sql(sql)
    print("✅ Database 'paimon.demo' created")
    print("💡 Paimon stores data in the warehouse directory configured in Spark")
    
    # Create Paimon table with primary key
    print("\n📝 Step 2: Create a Paimon table with a PRIMARY KEY")
    print("-" * 50)
    print("📚 Primary keys enable UPSERT operations (update-or-insert)")
    sql = """
        CREATE TABLE IF NOT EXISTS paimon.demo.employees (
            id BIGINT,
            name STRING,
            department STRING,
            salary INT,
            hire_date DATE
        ) TBLPROPERTIES (
            'primary-key' = 'id'
        )
    """
    print(f"SQL> {sql.strip()}")
    spark.sql(sql)
    print("✅ Table 'employees' created with primary key on 'id' column")
    print("💡 TBLPROPERTIES ('primary-key' = 'id') defines the primary key")
    
    # Load sample data
    print("\n📝 Step 3: Insert initial data into the Paimon table")
    print("-" * 50)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    employees_csv = os.path.join(base_dir, "data", "employees.csv")
    
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(employees_csv)
    print("Loading from: data/employees.csv")
    print("Schema inferred from CSV:")
    df.printSchema()
    
    sql = "INSERT INTO paimon.demo.employees"
    print(f"SQL> {sql}  -- using DataFrame")
    df.write.mode("overwrite").insertInto("paimon.demo.employees")
    print("✅ Data loaded successfully")
    
    # Query the data
    print("\n📝 Step 4: Query the Paimon table")
    print("-" * 50)
    sql = "SELECT * FROM paimon.demo.employees ORDER BY id"
    print(f"SQL> {sql}")
    result = spark.sql(sql)
    result.show()
    print("💡 Paimon tables are queried just like regular Spark tables")
    
    return result

def demo_paimon_upserts(spark):
    """Demonstrate Paimon's UPSERT capabilities"""
    
    print("\n" + "="*70)
    print("🔄 DEMO 2: UPSERT Operations (Primary Key in Action)")
    print("="*70)
    print("📚 Key Concept: Primary keys enable automatic UPSERT")
    print("   - INSERT with existing key → UPDATE")
    print("   - INSERT with new key → INSERT")
    print("="*70)
    
    # Show current data
    print("\n📝 Step 1: View current data (before UPSERT)")
    print("-" * 50)
    sql = "SELECT id, name, salary FROM paimon.demo.employees ORDER BY id"
    print(f"SQL> {sql}")
    spark.sql(sql).show()
    print("Note: Employee id=2 is 'Bob Smith' with salary 75000")
    
    # Perform UPSERT (update existing + insert new)
    print("\n📝 Step 2: Perform UPSERT operation")
    print("-" * 50)
    print("📚 Inserting rows with id=2 (EXISTS) and id=6 (NEW)")
    sql = """
        INSERT INTO paimon.demo.employees VALUES
        (2, 'Bob Smith Jr.', 'Marketing', 80000, DATE '2023-02-20'),  -- Update existing
        (6, 'Frank Miller', 'Engineering', 110000, DATE '2023-06-01') -- Insert new
    """
    print(f"SQL> {sql.strip()}")
    spark.sql(sql)
    print("✅ UPSERT completed")
    print("💡 Notice: We used INSERT, but Paimon automatically:")
    print("   • UPDATED the row where id=2 (Bob → Bob Smith Jr., 75k → 80k)")
    print("   • INSERTED the new row where id=6 (Frank Miller)")
    
    # Show updated data
    print("\n📝 Step 3: Verify the UPSERT result")
    print("-" * 50)
    sql = "SELECT id, name, salary FROM paimon.demo.employees ORDER BY id"
    print(f"SQL> {sql}")
    spark.sql(sql).show()
    print("✅ Row id=2 was UPDATED, Row id=6 was INSERTED")
    
    # Show Paimon's change tracking
    print("\n📝 Step 4: Inspect table metadata")
    print("-" * 50)
    sql = "DESCRIBE EXTENDED paimon.demo.employees"
    print(f"SQL> {sql}")
    spark.sql(sql).select("col_name", "data_type").show(truncate=False)
    print("💡 DESCRIBE EXTENDED shows table properties including primary key")

def demo_paimon_queries(spark):
    """Demonstrate various Paimon query capabilities"""
    
    print("\n" + "="*70)
    print("🔍 DEMO 3: Querying Paimon Tables")
    print("="*70)
    print("📚 Key Concept: Paimon supports full SQL query capabilities")
    print("="*70)
    
    # Aggregation queries
    print("\n📝 Query 1: Aggregation - Department salary analysis")
    print("-" * 50)
    sql = """
        SELECT 
            department,
            COUNT(*) as employee_count,
            AVG(salary) as avg_salary,
            MAX(salary) as max_salary
        FROM paimon.demo.employees 
        GROUP BY department
        ORDER BY avg_salary DESC
    """
    print(f"SQL> {sql.strip()}")
    spark.sql(sql).show()
    
    # Filtering queries
    print("\n📝 Query 2: Filter - High-salary employees (>90k)")
    print("-" * 50)
    sql = """
        SELECT name, department, salary 
        FROM paimon.demo.employees 
        WHERE salary > 90000 
        ORDER BY salary DESC
    """
    print(f"SQL> {sql.strip()}")
    spark.sql(sql).show()
    
    # Date-based queries
    print("\n📝 Query 3: Date filter - Recent hires (2023)")
    print("-" * 50)
    sql = """
        SELECT name, department, hire_date 
        FROM paimon.demo.employees 
        WHERE year(hire_date) = 2023 
        ORDER BY hire_date
    """
    print(f"SQL> {sql.strip()}")
    spark.sql(sql).show()
    print("💡 All standard Spark SQL functions work with Paimon tables")

def main():
    """Main demo function"""
    
    print("\n" + "="*70)
    print("🚀 Apache Paimon Demo - Table Format for Data Lakes")
    print("="*70)
    print("📚 What is Apache Paimon?")
    print("   • Open-source table format for data lakes")
    print("   • Supports ACID transactions with primary keys")
    print("   • Built on file formats (Parquet, ORC, Avro)")
    print("   • Integrates with Spark, Flink, Trino, Hive")
    print("="*70)
    print("\n📋 This demo will demonstrate:")
    print("   1. Creating Paimon tables with primary keys")
    print("   2. UPSERT operations (update-or-insert)")
    print("   3. Standard SQL queries on Paimon tables")
    print("="*70)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        print("\n✅ Spark session with Paimon catalog created successfully!")
        
        # Run demos
        demo_paimon_basics(spark)
        demo_paimon_upserts(spark)
        demo_paimon_queries(spark)
        
        print("\n" + "="*70)
        print("🎉 Demo Completed Successfully!")
        print("="*70)
        print("\n📚 Key Takeaways:")
        print("   1. Primary keys enable ACID UPSERT operations")
        print("   2. INSERT automatically becomes UPDATE for existing keys")
        print("   3. Standard Spark SQL works with Paimon tables")
        print("   4. Paimon stores metadata alongside data files")
        print("\n📖 Next Steps:")
        print("   • Run 'make run_paimon_and_iceberg_cross_platform_demo'")
        print("   • Explore Paimon's Iceberg compatibility feature")
        print("="*70)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Demo failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        if 'spark' in locals():
            spark.stop()
            print("\n🛑 Spark session stopped")

if __name__ == "__main__":
    sys.exit(main())
