
import kagglehub
import os
import pandas as pd
import oracledb

# -----------------------------------
# 1. Download Kaggle dataset
# -----------------------------------

path = kagglehub.dataset_download(
    "ksabishek/massive-bank-dataset-1-million-rows"
)

print("Dataset location:", path)
print("Files:", os.listdir(path))


# -----------------------------------
# 2. Find Excel file
# -----------------------------------

xlsx_files = [
    f for f in os.listdir(path)
    if f.lower().endswith(".xlsx")
]

if not xlsx_files:
    raise FileNotFoundError("No XLSX file found.")

xlsx_path = os.path.join(path, xlsx_files[0])

print("Using Excel:", xlsx_path)


# -----------------------------------
# 3. Read Excel
# -----------------------------------

df = pd.read_excel(xlsx_path)

print("Rows:", len(df))
print("Columns:", df.columns.tolist())

# Convert Date column to Python datetime
# Convert Date column to Python datetime
df["Date"] = pd.to_datetime(df["Date"])

# Add synthetic numeric primary key, required for Debezium
df.insert(0, "id", range(1, len(df) + 1))

print("Excel loaded successfully!")


# -----------------------------------
# 4. Connect to Oracle
# -----------------------------------

conn = oracledb.connect(
    user="bankuser",
    password="bankpass",
    dsn="localhost:1521/XEPDB1"
)

cursor = conn.cursor()

print("Connected to Oracle!")


# -----------------------------------
# 5. Create Oracle table
# -----------------------------------

try:
    cursor.execute("""
        CREATE TABLE bank_transactions (
            id                NUMBER(12) PRIMARY KEY,
            transaction_date  DATE,
            domain            VARCHAR2(100),
            location          VARCHAR2(100),
            value             NUMBER,
            transaction_count NUMBER
        )
    """)
    print("Oracle table created!")
except oracledb.DatabaseError as e:
    if "ORA-00955" not in str(e):
        raise
    print("Table already exists, skipping create.")

cursor.execute("ALTER TABLE bank_transactions ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
conn.commit()
print("Supplemental logging enabled!")


# -----------------------------------
# 6. Prepare INSERT
# -----------------------------------

sql = """
    INSERT INTO bank_transactions (
        id,
        transaction_date,
        domain,
        location,
        value,
        transaction_count
    )
    VALUES (
        :1,
        :2,
        :3,
        :4,
        :5,
        :6
    )
"""


# -----------------------------------
# 7. Insert in batches
# -----------------------------------

batch_size = 10000

total_rows = len(df)

for start in range(0, total_rows, batch_size):

    end = min(start + batch_size, total_rows)

    batch = df.iloc[start:end]

    rows = list(
        batch.itertuples(index=False, name=None)
    )

    cursor.executemany(sql, rows)

    conn.commit()

    print(f"Inserted {end:,} / {total_rows:,} rows")


# -----------------------------------
# 8. Close connection
# -----------------------------------

cursor.close()
conn.close()

print("Done!")
