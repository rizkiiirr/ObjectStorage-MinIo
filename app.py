import streamlit as st
from minio import Minio
import io

client = Minio(
    "127.0.0.1:9000",
    access_key="admin",
    secret_key="rizki12345",
    secure=False
)
BUCKET_NAME = "bucket-tugas"

if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)

st.set_page_config(page_title="Tugas Object Storage", layout="wide")

st.sidebar.title("Navigasi")
pilihan_halaman = st.sidebar.radio(
    "Pilih Menu:", 
    ["Upload Data", "Pencarian Gambar"]
)

if pilihan_halaman == "Upload Data":
    st.title("Upload Data ke Data Lake")
    st.markdown("---")
    
    with st.container():
        uploaded_file = st.file_uploader("Pilih File Gambar", type=['jpg', 'png', 'jpeg'])
        metadata_input = st.text_input("Metadata / Tag (Contoh: Pemandangan, Kucing, Laporan)")
        
        if st.button("Simpan ke Object Storage", type="primary"):
            if uploaded_file and metadata_input:
                try:
                    data = uploaded_file.getvalue()
                    data_stream = io.BytesIO(data)
                    tags = {"kategori": metadata_input}
                    
                    client.put_object(
                        BUCKET_NAME,
                        uploaded_file.name,
                        data_stream,
                        length=len(data),
                        content_type=uploaded_file.type,
                        metadata=tags
                    )
                    st.success(f"Berhasil upload: **{uploaded_file.name}**")
                    st.info(f"Metadata tersimpan: `{metadata_input}`")
                except Exception as e:
                    st.error(f"Gagal upload: {e}")
            else:
                st.warning("Harap lengkapi file dan metadata!")

elif pilihan_halaman == "Pencarian Gambar":
    st.title("Cari Data Unstructured")
    st.markdown("---")
    
    keyword = st.text_input("Masukkan kata kunci pencarian (berdasarkan Nama File atau Metadata):")
    
    if keyword: 
        st.write(f"Hasil pencarian untuk: **'{keyword}'**")
        
        try:
            objects = client.list_objects(BUCKET_NAME)
            found_count = 0
            
            cols = st.columns(3)
            
            for obj in objects:
                stat = client.stat_object(BUCKET_NAME, obj.object_name)
                meta_val = stat.metadata.get("x-amz-meta-kategori", "")
                
                if keyword.lower() in obj.object_name.lower() or keyword.lower() in meta_val.lower():
                    response = client.get_object(BUCKET_NAME, obj.object_name)
                    img_data = response.read()
                    
                    with cols[found_count % 3]:
                        st.image(img_data, use_container_width=True)
                        st.markdown(f"** {obj.object_name}**")
                        st.caption(f"Tag: {meta_val}")
                        st.divider()
                    
                    response.close()
                    response.release_conn()
                    found_count += 1
            
            if found_count == 0:
                st.warning("Tidak ditemukan gambar yang cocok.")
                
        except Exception as e:
            st.error(f"Terjadi kesalahan koneksi: {e}")