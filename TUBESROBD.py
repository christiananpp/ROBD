import streamlit as st
import json
import time
from pymongo import MongoClient, ASCENDING, DESCENDING
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

st.set_page_config(layout="wide")
st.title("🧪 Multi-DB Query & Indexing Tester (MongoDB & Cassandra)")

# --- Koneksi MongoDB
def connect_mongo():
    client = MongoClient("mongodb://localhost:27017")
    db = client["salon_db"]
    return db

# --- Koneksi Cassandra
def connect_cassandra():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect("salonapp")
    return session

# =========================
# PILIH DATABASE
# =========================
db_option = st.selectbox("Pilih Database", ["MongoDB", "Cassandra", "Aggregator"])

# =========================
# MONGODB MODE
# =========================
if db_option == "MongoDB":
    st.subheader("🧩 MongoDB Tester")
    db = connect_mongo()

    collection_name = st.text_input("Masukkan nama koleksi:", value="customers")

    if collection_name not in db.list_collection_names():
        st.warning("Koleksi tidak ditemukan.")
    else:
        # --- Query MongoDB
        st.markdown("### 🔍 Pipeline Query")
        pipeline_input = st.text_area("Masukkan pipeline MongoDB (JSON list)", '[{"$limit": 10}]')

        if st.button("Jalankan Query MongoDB"):
            try:
                pipeline = json.loads(pipeline_input)
                start = time.perf_counter()
                result = list(db[collection_name].aggregate(pipeline))
                duration = time.perf_counter() - start
                st.success(f"Waktu eksekusi: {duration:.4f} detik")
                st.write(result)
            except Exception as e:
                st.error(f"Error: {e}")

        # --- Index MongoDB
        st.markdown("### 🛠️ Index MongoDB")
        col_sample = db[collection_name].find_one()
        if col_sample:
            fields = list(col_sample.keys())
            field = st.selectbox("Pilih field index", fields)
            order = st.radio("Urutan index", ["Ascending", "Descending"])

            if st.button("Buat Index"):
                direction = ASCENDING if order == "Ascending" else DESCENDING
                index_name = db[collection_name].create_index([(field, direction)])
                st.success(f"Index dibuat: {index_name}")

        if st.button("Lihat Semua Index"):
            st.json(db[collection_name].index_information())

        if st.button("Hapus Semua Index Kecuali _id"):
            for idx in db[collection_name].index_information():
                if idx != "_id_":
                    db[collection_name].drop_index(idx)
            st.success("Semua index non-_id_ dihapus.")

# =========================
# CASSANDRA MODE
# =========================
elif db_option == "Cassandra":
    st.subheader("🧩 Cassandra Tester")
    session = connect_cassandra()

    # Ambil nama tabel
    rows = session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{session.keyspace}'")
    tabels = [r.table_name for r in rows]
    table = st.selectbox("Pilih tabel:", tabels)

    # --- Query CQL
    st.markdown("### 🔍 Query CQL")
    default_q = f"SELECT * FROM {table} LIMIT 10;"
    cql_input = st.text_area("Masukkan CQL:", value=default_q)

    if st.button("Jalankan Query Cassandra"):
        try:
            stmt = SimpleStatement(cql_input)
            start = time.perf_counter()
            result = list(session.execute(stmt))
            duration = time.perf_counter() - start
            st.success(f"Waktu eksekusi: {duration:.4f} detik")
            st.dataframe([dict(r._asdict()) for r in result])
        except Exception as e:
            st.error(f"Gagal: {e}")

    # --- Indexing
    st.markdown("### 🛠️ Index Cassandra")
    rows = session.execute(f"SELECT column_name FROM system_schema.columns WHERE keyspace_name='{session.keyspace}' AND table_name='{table}'")
    columns = [r.column_name for r in rows]
    field = st.selectbox("Pilih kolom untuk index", columns)

    if st.button("Buat Index Cassandra"):
        try:
            index_name = f"idx_{table}_{field}"
            session.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({field});")
            st.success(f"Index dibuat: {index_name}")
        except Exception as e:
            st.error(f"Gagal: {e}")

    if st.button("Lihat Index Cassandra"):
        idx = session.execute(f"SELECT index_name FROM system_schema.indexes WHERE keyspace_name='{session.keyspace}' AND table_name='{table}'")
        st.json([r.index_name for r in idx])

    if st.button("Hapus Semua Index Cassandra di Tabel Ini"):
        try:
            idx = session.execute(f"SELECT index_name FROM system_schema.indexes WHERE keyspace_name='{session.keyspace}' AND table_name='{table}'")
            for r in idx:
                session.execute(f"DROP INDEX IF EXISTS {r.index_name};")
            st.success("Semua index dihapus.")
        except Exception as e:
            st.error(f"Gagal menghapus index: {e}")

# =========================
# AGGREGATOR MODE
# =========================
elif db_option == "Aggregator":
    st.subheader("🔗 Aggregator View (MongoDB + Cassandra)")

    db = connect_mongo()
    session = connect_cassandra()

    mongo_collection = "customers"
    if mongo_collection not in db.list_collection_names():
        st.warning("Koleksi 'customers' tidak ditemukan di MongoDB.")
    else:
        mongo_data = list(db[mongo_collection].find({}, {"_id": 0}))
        if not mongo_data:
            st.warning("Data pelanggan tidak ditemukan.")
        else:
            st.success(f"{len(mongo_data)} data customer ditemukan.")
            with st.expander("Contoh data customer:"):
                st.write(mongo_data[:1])

            try:
                cassandra_rows = session.execute("SELECT * FROM appointments")
                cassandra_data = [dict(r._asdict()) for r in cassandra_rows]

                if not cassandra_data:
                    st.warning("Data appointment tidak ditemukan.")
                else:
                    st.success(f"{len(cassandra_data)} data appointment ditemukan.")
                    with st.expander("Contoh data appointment:"):
                        st.write(cassandra_data[:1])

                    # ⏱️ Mulai pengukuran waktu agregasi
                    start_agg_time = time.perf_counter()

                    cassandra_by_cust = {item["id_cust"]: item for item in cassandra_data}

                    hasil_agregasi = []
                    for cust in mongo_data:
                        id_cust = cust.get("id_cust")
                        appointment = cassandra_by_cust.get(id_cust)
                        if appointment:
                            hasil_agregasi.append({
                                "ID Customer": id_cust,
                                "Nama Customer": cust.get("nama_cust"),
                                "Nama Pegawai": cust.get("nama_pegawai"),
                                "Pelayanan": appointment.get("pelayanan"),
                                "Tanggal": appointment.get("tanggal"),
                                "Total": appointment.get("total"),
                                "Metode Pembayaran": appointment.get("cara_pembayaran"),
                            })

                    agg_duration = time.perf_counter() - start_agg_time
                    st.markdown("### ⚙️ Filter Agregasi Dinamis")
                    st.info(f"Waktu eksekusi agregasi data: {agg_duration:.4f} detik")

                    semua_nama = sorted(set(row["Nama Customer"] for row in hasil_agregasi))
                    semua_pelayanan = sorted(set(row["Pelayanan"] for row in hasil_agregasi))

                    nama_dipilih = st.multiselect("Filter Nama Customer:", semua_nama)
                    pelayanan_dipilih = st.multiselect("Filter Jenis Pelayanan:", semua_pelayanan)
                    min_total, max_total = st.slider("Filter Harga Total:", 0, 300000, (0, 300000), step=5000)

                    hasil_filtered = [
                        row for row in hasil_agregasi
                        if (not nama_dipilih or row["Nama Customer"] in nama_dipilih)
                        and (not pelayanan_dipilih or row["Pelayanan"] in pelayanan_dipilih)
                        and (min_total <= row["Total"] <= max_total)
                    ]

                    st.markdown("### 📊 Hasil Agregasi (Filtered)")
                    st.dataframe(hasil_filtered, use_container_width=True)

                    if hasil_filtered:
                        st.markdown("### 🧠 Group By untuk Analisis Lanjutan")
                        kolom_tersedia = ["Nama Customer", "Pelayanan", "Metode Pembayaran", "Nama Pegawai", "Tanggal"]
                        kolom_grup = st.multiselect("Pilih Kolom untuk Group By:", kolom_tersedia, default=["Nama Customer", "Pelayanan"])

                        if kolom_grup:
                            start_time = time.perf_counter()

                            analisis_agg = {}
                            for row in hasil_filtered:
                                key = tuple(row[k] for k in kolom_grup)
                                if key not in analisis_agg:
                                    analisis_agg[key] = {
                                        "Total Transaksi": 0,
                                        "Total Pembayaran": 0,
                                        "Metode Pembayaran": {}
                                    }
                                analisis_agg[key]["Total Transaksi"] += 1
                                analisis_agg[key]["Total Pembayaran"] += row["Total"]
                                metode = row["Metode Pembayaran"]
                                analisis_agg[key]["Metode Pembayaran"][metode] = analisis_agg[key]["Metode Pembayaran"].get(metode, 0) + 1

                            duration = time.perf_counter() - start_time

                            st.markdown("### 📈 Hasil Analisis Agregasi")
                            for key_tuple, data in analisis_agg.items():
                                label = " | ".join([f"{col}: {val}" for col, val in zip(kolom_grup, key_tuple)])
                                st.markdown(f"**{label}**")
                                st.write(f"- Total Transaksi: {data['Total Transaksi']}")
                                st.write(f"- Total Pembayaran: Rp{data['Total Pembayaran']:,.0f}")
                                st.write("- Frekuensi Metode Pembayaran:")
                                st.json(data["Metode Pembayaran"])
                                st.write("---")

                            st.info(f"Waktu eksekusi analisis: {duration:.4f} detik")
                        else:
                            st.warning("Pilih minimal satu kolom untuk analisis.")
            except Exception as e:
                st.error(f"Gagal mengambil layanan dari Cassandra: {e}")
