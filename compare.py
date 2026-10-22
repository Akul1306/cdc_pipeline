import oracledb, psycopg2
from decimal import Decimal

ora = oracledb.connect(user="bankuser", password="bankpass", dsn="localhost:1521/XEPDB1")
pg = psycopg2.connect("dbname=target user=target password=target host=localhost")

def one(conn, sql):
    cur = conn.cursor(); cur.execute(sql); r = cur.fetchone(); cur.close(); return r

q = "SELECT COUNT(*), SUM(id), SUM(value), SUM(transaction_count) FROM {}"
o = [Decimal(str(x)) for x in one(ora, q.format("bankuser.bank_transactions"))]
p = [Decimal(str(x)) for x in one(pg, q.format("bank_transactions"))]
for name, a, b in zip(["count", "sum(id)", "sum(value)", "sum(txn_count)"], o, p):
    print(f"{name:15} oracle={a}  postgres={b}  {'OK' if a == b else 'MISMATCH'}")

def ids(conn, sql):
    cur = conn.cursor(); cur.execute(sql); s = {int(r[0]) for r in cur}; cur.close(); return s

so = ids(ora, "SELECT id FROM bankuser.bank_transactions")
sp = ids(pg, "SELECT id FROM bank_transactions")
print("missing in postgres:", len(so - sp), "| extra in postgres:", len(sp - so))
print("PASS" if o == p and so == sp else "FAIL")