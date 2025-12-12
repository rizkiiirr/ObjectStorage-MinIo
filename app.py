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

st.set_page_config(page_title="Tugas Object Storage", layout="wide")
st.title("Tugas BI: Object Storage & Metadata")

col_upload, col_search = st.columns(2)

with col_upload:
    st.header("Upload Gambar")
    uploaded_file = st.file_uploader("Pilih File", type=['jpg', 'png', 'jpeg'])
    
    metadata_input = st.text_input("Metadata / Tag (Misal: Pemandangan, Kucing)")
    
    if st.button("Simpan ke Object Storage"):
        if uploaded_file and metadata_input:
            data = uploaded_file.getvalue()
            data_stream = io.BytesIO(data)
            
            tags = {"kategori": metadata_input}
            
            try:
                client.put_object(
                    BUCKET_NAME,
                    uploaded_file.name,
                    data_stream,
                    length=len(data),
                    content_type=uploaded_file.type,
                    metadata=tags
                )
                st.success(f"Berhasil upload: {uploaded_file.name}")
            except Exception as e:
                st.error(f"Gagal: {e}")
        else:
            st.warning("File dan Metadata harus diisi!")

with col_search:
    st.header("Cari Gambar")
    keyword = st.text_input("Cari berdasarkan Metadata/Nama File:")
    
    if st.button("Cari"):
        objects = client.list_objects(BUCKET_NAME)
        found = False
        
        cols = st.columns(2)
        i = 0
        
        for obj in objects:
            stat = client.stat_object(BUCKET_NAME, obj.object_name)
            meta_val = stat.metadata.get("x-amz-meta-kategori", "")
            
            if keyword.lower() in obj.object_name.lower() or keyword.lower() in meta_val.lower():
                found = True
                response = client.get_object(BUCKET_NAME, obj.object_name)
                
                with cols[i % 2]:
                    st.image(response.read(), caption=obj.object_name, use_container_width=True)
                    st.info(f"Tag: {meta_val}")
                
                response.close()
                response.release_conn()
                i += 1
                
        if not found:
            st.write("Tidak ditemukan.")