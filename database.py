import os
import pandas as pd
from sqlalchemy import create_engine, text

# Get PostgreSQL credentials from environment variables
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "superstore_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "Sentinel123")

# Create SQLAlchemy Engine
engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

try:
    df = pd.read_csv("staging_superstore.csv")
except pd.errors.EmptyDataError:
    df = pd.DataFrame()

if df.empty:
    print("No records to load into Database. Skipping.")
    exit(0)

# Ensure date columns are parsed as datetime so to_sql creates them as timestamps
if 'order_date' in df.columns:
    df['order_date'] = pd.to_datetime(df['order_date'])
if 'ship_date' in df.columns:
    df['ship_date'] = pd.to_datetime(df['ship_date'])

# Create a temporary table in PostgreSQL
temp_table_name = "superstore_temp_staging"
with engine.begin() as conn:
    df.to_sql(temp_table_name, conn, if_exists="replace", index=False)

# Build UPSERT query
final_table_name = "superstore_sales_final"
columns = list(df.columns)
update_set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'row_id'])

# First ensure final table exists
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {final_table_name} AS 
SELECT * FROM {temp_table_name} WHERE 1=0;
"""

# Ensure row_id is uniquely constrained if we just created it
alter_table_sql = f"""
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{final_table_name}_pkey') THEN
        ALTER TABLE {final_table_name} ADD PRIMARY KEY (row_id);
    END IF;
END $$;
"""

# Upsert statement
upsert_sql = f"""
INSERT INTO {final_table_name} ({', '.join(columns)})
SELECT {', '.join(columns)} FROM {temp_table_name}
ON CONFLICT (row_id) DO UPDATE SET
{update_set_clause};
"""

# Drop temp table
drop_temp_sql = f"DROP TABLE {temp_table_name};"

# Execute transactions
with engine.begin() as conn:
    conn.execute(text(create_table_sql))
    conn.execute(text(alter_table_sql))
    conn.execute(text(upsert_sql))
    conn.execute(text(drop_temp_sql))
    
    # Get total count for logging
    result = conn.execute(text(f"SELECT COUNT(*) FROM {final_table_name}"))
    count = result.fetchone()[0]

print("=" * 60)
print("Database Loaded Successfully")
print(f"Total Records in {final_table_name} : {count}")
print("=" * 60)