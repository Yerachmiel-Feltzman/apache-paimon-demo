import pandas as pd
import os

# Create sample employee data
employees = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice Johnson', 'Bob Smith', 'Carol Davis', 'David Wilson', 'Eve Brown'],
    'department': ['Engineering', 'Marketing', 'Engineering', 'Sales', 'Engineering'],
    'salary': [95000, 75000, 105000, 85000, 98000],
    'hire_date': ['2023-01-15', '2023-02-20', '2023-03-10', '2023-04-05', '2023-05-12']
})

# Create sample department data  
departments = pd.DataFrame({
    'name': ['Engineering', 'Marketing', 'Sales', 'HR'],
    'budget': [2000000, 800000, 1200000, 500000],
    'manager': ['Alice Johnson', 'Marketing Director', 'Sales Director', 'HR Director']
})

# Save as CSV for easy loading
os.makedirs('data', exist_ok=True)
employees.to_csv('data/employees.csv', index=False)
departments.to_csv('data/departments.csv', index=False)
print("✅ Sample data created")