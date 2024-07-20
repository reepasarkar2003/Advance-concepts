import pandas as pd
from sqlalchemy import create_engine

# Create source and destination SQLite in-memory databases
source_engine = create_engine('sqlite:///:memory:')
dest_engine = create_engine('sqlite:///:memory:')

# Populate source database with sample data
df1 = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})
df1.to_sql('table1', source_engine, index=False, if_exists='replace')

df2 = pd.DataFrame({
    'id': [4, 5, 6],
    'name': ['David', 'Eve', 'Frank'],
    'salary': [50000, 60000, 70000]
})
df2.to_sql('table2', source_engine, index=False, if_exists='replace')

# Define the tables and columns to copy
tables_columns = {
    "table1": ["id", "name"],
    "table2": ["name", "salary"],
}

for table, columns in tables_columns.items():
    # Load selected columns from the source table
    df = pd.read_sql(f"SELECT {', '.join(columns)} FROM {table}", source_engine)
    
    # Write data to the destination table
    df.to_sql(table, dest_engine, if_exists='replace', index=False)
    print(f"Table {table} with columns {', '.join(columns)} successfully copied from source to destination database")
