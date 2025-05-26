### --- aggregator_app.py ---
import streamlit as st
from pymongo import MongoClient
from cassandra.cluster import Cluster
from datetime import datetime

# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client["salon_db"]
appointments = db["appointments"]

# Cassandra Setup
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('salon_keyspace')

st.title("💇 Salon Query Aggregator by ID Pegawai")

# Input ID Pegawai dan Tanggal
id_pegawai = st.text_input("Masukkan ID Pegawai:")
tanggal = st.date_input("Pilih Tanggal Pelayanan:")

if id_pegawai:
    # Query Cassandra - Total Transaksi
    cassandra_query = f"""
    SELECT * FROM transaksi_penjualan WHERE id_pegawai = '{id_pegawai}' ALLOW FILTERING
    """
    transaksi = session.execute(cassandra_query)
    transaksi_list = [dict(row._asdict()) for row in transaksi]

    total_transaksi = sum(
        float(row['harga']) 
        for row in transaksi_list 
        if 'harga' in row and row['harga'] not in [None, '']
    )

    st.subheader("Total Transaksi Pegawai Ini:")
    st.write(f"Rp {total_transaksi:,.2f}")
    st.dataframe(transaksi_list)

    # Query MongoDB - Pelanggan yang pernah dilayani oleh pegawai pada tanggal tertentu
    tanggal_str = tanggal.strftime("%Y-%m-%d")
    pelanggan_cursor = appointments.find({
        "id_pegawai": id_pegawai,
        "tanggal": tanggal_str
    })
    pelanggan_set = set()
    for doc in pelanggan_cursor:
        pelanggan_set.add(doc.get("nama_customer", "Tidak diketahui"))

    st.subheader("Pelanggan yang Dilayani pada Tanggal Tersebut:")
    st.write(list(pelanggan_set))
