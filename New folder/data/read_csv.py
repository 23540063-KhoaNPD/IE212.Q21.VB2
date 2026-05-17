import pandas as pd

chunk_size = 1000  # Nên để khoảng 1000-5000 dòng để tối ưu RAM
file_path = "final_dataset.csv"

# Đọc file theo chunk
chunks = pd.read_csv(file_path, chunksize=chunk_size)

print("🚀 Đang bắt đầu lọc dữ liệu (Bỏ qua các dòng 'ddos')...")

for i, chunk in enumerate(chunks):
    # 1. Làm sạch tên cột (Xóa khoảng trắng ở đầu/cuối tên cột)
    chunk.columns = chunk.columns.str.strip()
    
    # 2. Lọc: Giữ lại các dòng có Label KHÁC 'ddos'
    # .str.strip() và .str.lower() giúp loại bỏ các biến thể như " ddos", "DDOS "
    filtered_chunk = chunk[chunk['Label'].str.strip().str.lower() != 'ddos']
    
    # Kiểm tra nếu chunk sau khi lọc vẫn còn dữ liệu
    if not filtered_chunk.empty:
        print(f"✅ Chunk {i}: Còn lại {len(filtered_chunk)} dòng sau khi lọc.")
        
        # Xử lý dữ liệu đã lọc ở đây (Ví dụ: Gửi vào Kafka hoặc in ra)
        # print(filtered_chunk[['Src IP', 'Dst IP', 'Label']].head())
    else:
        print(f"ℹ️ Chunk {i}: Toàn bộ là 'ddos', đã bỏ qua.")

print("🏁 Hoàn thành đọc file.")