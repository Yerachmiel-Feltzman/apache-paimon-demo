#!/bin/bash

echo "This script will clean up the environment by removing:
 - the jars directory
 - the warehouse directory
 - the data directory"
echo "Should we proceed? (y/n)"
read proceed
if [[ ! "$proceed" =~ ^([yY][eE][sS]?|[yY])$ ]]; then
    echo "🛑 Cleanup cancelled."
    exit 1
fi

echo "🗑️  Cleaning up..."

# Check if we are running from the same directory as setup.sh
if [ ! -f "setup.sh" ]; then
    echo "❌ You must run cleanup.sh from the same directory as setup.sh."
    exit 1
fi

rm -rf data
rm -rf jars
rm -rf warehouse

echo "🎉 Cleanup complete!"
tree .