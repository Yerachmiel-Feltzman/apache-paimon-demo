#!/bin/bash

# Paimon & Iceberg Demo Setup Script
# ==================================
# This script sets up the environment for demoing Paimon and Iceberg

set -e  # Exit on any error

# Define compatible versions (tested working combination)
SPARK_VERSION="3.4" # same as pyspark version
SCALA_VERSION="2.12" 
PAIMON_VERSION="1.3.0"
ICEBERG_VERSION="1.10.0"

echo "📦 Using versions:"
echo "  - Spark: ${SPARK_VERSION}"
echo "  - Paimon: ${PAIMON_VERSION}"
echo "  - Iceberg: ${ICEBERG_VERSION}"
echo ""


echo "=============================================="
echo "🚀 Setting up Paimon & Iceberg Demo Environment"
echo "=============================================="

echo "⚠️ Running from ${PWD}"

# Check if Java is installed
if ! command -v java &> /dev/null; then
    echo "❌ Java is not installed. Please install Java 17 or higher."
    echo "   I recommend using SDKMAN to install Java so it will pick up the correct version automatically from the .sdkmanrc file."
    exit 1
fi

# Check if Java version is 17 or higher
if ! java -version 2>&1 | grep -q "17"; then
    echo "❌ Java version is not 17 or higher. Please install Java 17 or higher."
    exit 1
fi

# Create directories
echo "🔨 Creating directories ..."
mkdir -p jars warehouse/{paimon,iceberg} data
echo "✅ Directories created"


# Download Paimon JAR
PAIMON_JAR="paimon-spark-${SPARK_VERSION}-${PAIMON_VERSION}.jar"
if [ ! -f "jars/${PAIMON_JAR}" ]; then
    echo "⬇️  Downloading Paimon JAR..."
    curl -L "https://repo1.maven.org/maven2/org/apache/paimon/paimon-spark-${SPARK_VERSION}/${PAIMON_VERSION}/${PAIMON_JAR}" \
         -o "jars/${PAIMON_JAR}"
    echo "✅ Paimon JAR downloaded"
else
    echo "✅ Paimon JAR already exists"
fi

# Download Iceberg JAR
ICEBERG_JAR="iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"
if [ ! -f "jars/${ICEBERG_JAR}" ]; then
    echo "⬇️  Downloading Iceberg JAR..."
    curl -L "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/${ICEBERG_JAR}" \
         -o "jars/${ICEBERG_JAR}"
    echo "✅ Iceberg JAR downloaded"
else
    echo "✅ Iceberg JAR already exists"
fi

# Download Paimon-Iceberg JAR (for REST Catalog support)
PAIMON_ICEBERG_JAR="paimon-iceberg-${PAIMON_VERSION}.jar"
if [ ! -f "jars/${PAIMON_ICEBERG_JAR}" ]; then
    echo "⬇️  Downloading Paimon-Iceberg JAR (for REST Catalog support)..."
    curl -L "https://repo1.maven.org/maven2/org/apache/paimon/paimon-iceberg/${PAIMON_VERSION}/${PAIMON_ICEBERG_JAR}" \
         -o "jars/${PAIMON_ICEBERG_JAR}"
    echo "✅ Paimon-Iceberg JAR downloaded"
    echo "   Note: This JAR enables REST Catalog integration"
else
    echo "✅ Paimon-Iceberg JAR already exists"
fi

# Install Python dependencies
echo "🐍 Setting up Python environment..."
if [ ! -d "env" ]; then
    echo "📦 Creating Python virtual environment in .venv ..."
    python3 -m venv .venv
    echo "✅ Virtual environment created"
else
    echo "✅ Python virtual environment already exists"
fi
source .venv/bin/activate
pip install -r requirements.txt
echo "✅ Python environment setup"

# Create sample data
echo "📊 Creating sample data..."
python sample-data.py
echo "✅ Sample data created"

echo ""
echo "🎉 Setup Complete!"
tree .

