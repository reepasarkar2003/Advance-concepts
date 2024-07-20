import pandas as pd
from sqlalchemy import create_engine
import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
import os

# Create an SQLite in-memory database and populate it with sample data
engine = create_engine('sqlite:///:memory:')
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})
df.to_sql('sample_table', engine, index=False, if_exists='replace')

# Load data from the table into a DataFrame
df = pd.read_sql("SELECT * FROM sample_table", engine)

# Write the DataFrame to CSV
csv_path = "output/sample_table.csv"
df.to_csv(csv_path, index=False)
print(f"Data successfully written to CSV at {csv_path}")

# Write the DataFrame to Parquet
parquet_path = "output/sample_table.parquet"
df.to_parquet(parquet_path, index=False)
print(f"Data successfully written to Parquet at {parquet_path}")

# Write the DataFrame to Avro
def write_avro(df, file_path):
    schema = {
        "doc": "Sample table schema",
        "name": "SampleTable",
        "namespace": "example.avro",
        "type": "record",
        "fields": [{"name": col, "type": ["null", "string"]} for col in df.columns]
    }
    records = df.to_dict(orient='records')
    with open(file_path, 'wb') as out:
        fastavro.writer(out, schema, records)

avro_path = "output/sample_table.avro"
write_avro(df, avro_path)
print(f"Data successfully written to Avro at {avro_path}")
