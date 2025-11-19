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
    
    print("\n📊 === PAIMON BASICS DEMO ===")
    
    # Create database
    print("\n1️⃣ Creating Paimon database...")
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon.demo")
    print("✅ Database created")
    
    # Create Paimon table with primary key
    print("\n2️⃣ Creating Paimon table with primary key...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS paimon.demo.employees (
            id BIGINT,
            name STRING,
            department STRING,
            salary INT,
            hire_date DATE
        ) TBLPROPERTIES (
            'primary-key' = 'id'
        )
    """)
    print("✅ Paimon table created with primary key")
    
    # Load sample data
    print("\n3️⃣ Loading sample data...")
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    employees_csv = os.path.join(base_dir, "data", "employees.csv")
    
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(employees_csv)
    df.write.mode("overwrite").insertInto("paimon.demo.employees")
    print("✅ Sample data loaded")
    
    # Query the data
    print("\n4️⃣ Querying Paimon table...")
    result = spark.sql("SELECT * FROM paimon.demo.employees ORDER BY id")
    result.show()
    
    return result

def demo_paimon_upserts(spark):
    """Demonstrate Paimon's UPSERT capabilities"""
    
    print("\n🔄 === PAIMON UPSERTS DEMO ===")
    
    # Show current data
    print("\n1️⃣ Current employee data:")
    spark.sql("SELECT id, name, salary FROM paimon.demo.employees ORDER BY id").show()
    
    # Perform UPSERT (update existing + insert new)
    print("\n2️⃣ Performing UPSERT operations...")
    spark.sql("""
        INSERT INTO paimon.demo.employees VALUES
        (2, 'Bob Smith Jr.', 'Marketing', 80000, DATE '2023-02-20'),  -- Update existing
        (6, 'Frank Miller', 'Engineering', 110000, DATE '2023-06-01') -- Insert new
    """)
    print("✅ UPSERT completed")
    
    # Show updated data
    print("\n3️⃣ Updated employee data:")
    spark.sql("SELECT id, name, salary FROM paimon.demo.employees ORDER BY id").show()
    
    # Show Paimon's change tracking
    print("\n4️⃣ Paimon table statistics:")
    spark.sql("DESCRIBE EXTENDED paimon.demo.employees").select("col_name", "data_type").show(truncate=False)

def demo_paimon_queries(spark):
    """Demonstrate various Paimon query capabilities"""
    
    print("\n🔍 === PAIMON QUERIES DEMO ===")
    
    # Aggregation queries
    print("\n1️⃣ Department salary analysis:")
    spark.sql("""
        SELECT 
            department,
            COUNT(*) as employee_count,
            AVG(salary) as avg_salary,
            MAX(salary) as max_salary
        FROM paimon.demo.employees 
        GROUP BY department
        ORDER BY avg_salary DESC
    """).show()
    
    # Filtering queries
    print("\n2️⃣ High-salary employees (>90k):")
    spark.sql("""
        SELECT name, department, salary 
        FROM paimon.demo.employees 
        WHERE salary > 90000 
        ORDER BY salary DESC
    """).show()
    
    # Date-based queries
    print("\n3️⃣ Recent hires (2023):")
    spark.sql("""
        SELECT name, department, hire_date 
        FROM paimon.demo.employees 
        WHERE year(hire_date) = 2023 
        ORDER BY hire_date
    """).show()

def main():
    """Main demo function"""
    
    print("🚀 Apache Paimon Demo")
    print("=" * 50)
    print("This demo will show you Paimon's key features:")
    print("• Primary key tables with ACID operations")
    print("• UPSERT capabilities")  
    print("• Query performance")
    print("• Integration with Spark SQL")
    print("=" * 50)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        print("✅ Spark session with Paimon created successfully!")
        
        # Run demos
        demo_paimon_basics(spark)
        demo_paimon_upserts(spark)
        demo_paimon_queries(spark)
        
        print("\n🎉 Paimon Demo Completed Successfully!")
        print("\n💡 Key Takeaways:")
        print("• Paimon provides ACID transactions with primary keys")
        print("• Write operations (UPSERT tested) work with Spark SQL")
        print("• Read operations work with Spark SQL")
        
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
