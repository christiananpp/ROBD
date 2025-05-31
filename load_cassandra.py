import pandas as pd
from cassandra.cluster import Cluster
from datetime import datetime

# Load data dari CSV
csv_path = "data_appointments_20000.csv"
df = pd.read_csv(csv_path)

# Koneksi ke Cassandra
cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

# Pilih keyspace yang sudah ada
session.set_keyspace("salonapp")

# Buat ulang tabel (pastikan sudah di-DROP dari cqlsh sebelumnya)
session.execute("""
CREATE TABLE IF NOT EXISTS appointments (
    id_appointment text PRIMARY KEY,
    id_cust text,
    id_pegawai text,
    pelayanan text,
    tanggal date,
    cara_pembayaran text,
    total int
)
""")

# Masukkan data ke tabel appointments
for _, row in df.iterrows():
    session.execute("""
        INSERT INTO appointments (id_appointment, id_cust, id_pegawai, pelayanan, tanggal, cara_pembayaran, total)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        row["id_appointment"],
        row["id_cust"],
        row["id_pegawai"],
        row["pelayanan"],
        datetime.strptime(row["tanggal"], "%Y-%m-%d").date(),
        row["cara_pembayaran"],
        int(row["total"])
    ))

print("✅ Data berhasil dimasukkan ke tabel appointments di Cassandra.")



