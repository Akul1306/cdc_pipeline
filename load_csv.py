import kagglehub
import os
import pandas as pd
import oracledb

# -----------------------------
# 1. Download Kaggle dataset
# -----------------------------
path = kagglehub.dataset_download("neelesh0602/bankcsv")

csv_path = os.path.join(path, "banking.csv")

print("CSV:", csv_path)

# -----------------------------
# 2. Read CSV
# -----------------------------
df = pd.read_csv(csv_path)

print("Rows:", len(df))
print("Columns:", df.columns.tolist())

# -----------------------------
# 3. Connect to Oracle
# -----------------------------
conn = oracledb.connect(
    user="bankuser",
    password="bankpass",
    dsn="localhost:1521/XEPDB1"
)

cursor = conn.cursor()

cursor.execute("""
    SELECT
        USER,
        SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'),
        SYS_CONTEXT('USERENV', 'SERVICE_NAME')
    FROM dual
""")

print("Oracle connection:", cursor.fetchone())

cursor = conn.cursor()

print("Connected to Oracle!")

# -----------------------------
# 4. Create table
# -----------------------------
cursor.execute("""
    CREATE TABLE banking (
        age NUMBER,
        job VARCHAR2(100),
        marital VARCHAR2(50),
        education VARCHAR2(100),
        default_status VARCHAR2(20),
        housing VARCHAR2(20),
        loan VARCHAR2(20),
        contact VARCHAR2(50),
        month VARCHAR2(20),
        day_of_week VARCHAR2(20),
        duration NUMBER,
        campaign NUMBER,
        pdays NUMBER,
        previous NUMBER,
        poutcome VARCHAR2(50),
        emp_var_rate NUMBER,
        cons_price_idx NUMBER,
        cons_conf_idx NUMBER,
        euribor3m NUMBER,
        nr_employed NUMBER,
        y NUMBER
    )
""")

# -----------------------------
# 5. Insert CSV rows
# -----------------------------
sql = """
    INSERT INTO banking (
        age,
        job,
        marital,
        education,
        default_status,
        housing,
        loan,
        contact,
        month,
        day_of_week,
        duration,
        campaign,
        pdays,
        previous,
        poutcome,
        emp_var_rate,
        cons_price_idx,
        cons_conf_idx,
        euribor3m,
        nr_employed,
        y
    )
    VALUES (
        :1, :2, :3, :4, :5, :6, :7, :8, :9, :10,
        :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21
    )
"""

rows = []

for row in df.itertuples(index=False, name=None):
    rows.append(row)

cursor.executemany(sql, rows)

conn.commit()

print(f"Inserted {len(rows)} rows into BANKING")

cursor.close()
conn.close()

print("Done!")