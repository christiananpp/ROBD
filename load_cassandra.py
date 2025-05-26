### --- load_cassandra.py ---
import pandas as pd
from cassandra.cluster import Cluster
from datetime import datetime

# Load data dari CSV
csv_path = "transaksi_penjualan_salon_20000.csv"
df = pd.read_csv(csv_path)

# Koneksi ke Cassandra
cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

# Set keyspace (pastikan sudah dibuat sebelumnya)
session.set_keyspace("salon_keyspace")

# Buat tabel jika belum ada
session.execute("""
CREATE TABLE IF NOT EXISTS transaksi_penjualan (
    id_transaksi text PRIMARY KEY,
    tanggal date,
    kategori_produk text,
    harga int,
    id_pegawai text
)
""")

# Buat index pada kolom id_pegawai dan tanggal
session.execute("CREATE INDEX IF NOT EXISTS ON transaksi_penjualan (id_pegawai)")
session.execute("CREATE INDEX IF NOT EXISTS ON transaksi_penjualan (tanggal)")

# Masukkan data ke tabel
for _, row in df.iterrows():
    session.execute("""
        INSERT INTO transaksi_penjualan (id_transaksi, tanggal, kategori_produk, harga, id_pegawai)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        row['id_transaksi'],
        datetime.strptime(row['tanggal'], "%m/%d/%Y").date(),
        row['kategori_produk'],
        int(row['harga']),
        row['id_pegawai']
    ))

print("\u2705 Data berhasil dimasukkan ke Cassandra.")