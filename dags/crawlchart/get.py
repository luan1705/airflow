from sqlalchemy import create_engine
from VNSFintech import * 
import concurrent.futures
from datetime import datetime

# Kết nối DB
engine = create_engine(
    'postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech',
    pool_size=20,
    max_overflow=30,
    pool_timeout=60
)

# Danh sách symbol lấy từ API
symbols = [
    'HNX Construction Index','HNX Financials Index','HNX Large Cap Index',
    'HNX Manufacturing Index','HNX Mid/Small Cap Index','HNX30','HNXIndex',
    'HNXUpcomIndex','UPCOM Large Index','UPCOM Medium Index','UPCOM Small Index',
    'VN100','VN30','VNALLSHARE','VNCOND','VNCONS','VNDIAMOND','VNENE','VNFIN',
    'VNFINLEAD','VNFINSELECT','VNHEAL','VNIND','VNINDEX','VNIT','VNMAT',
    'VNMIDCAP','VNREAL','VNSI','VNSMALLCAP','VNUTI','VNX50'
]

# Map tên API -> tên bảng
table_name_map = {
    "HNXUpcomIndex": "UpcomIndex"
}

# Map cột từ tiếng Việt sang tiếng Anh (camelCase)
column_map = {
    "Mã CP": "symbol",
    "Thời gian": "time",
    "Giá mở cửa": "openPrice",
    "Giá đóng cửa": "closePrice",
    "Giá cao nhất": "highPrice",
    "Giá thấp nhất": "lowPrice",
    "Khối lượng": "volume"
}

def get_stock(symbol):
    try:
        today = datetime.now().strftime('%Y-%m-%d')

        # Lấy dữ liệu lịch sử từ API
        stock = history(symbol=symbol, start='2006-01-01', end=today, time="minutes")

        # Kiểm tra dữ liệu trả về
        if stock is None or stock.empty:
            return f"⚠️ Không có dữ liệu cho {symbol}"

        # Đổi tên cột sang tiếng Anh (nếu có trong map)
        stock.rename(columns=column_map, inplace=True)

        # Lấy tên bảng trong DB (nếu có map thì dùng tên map, không thì giữ nguyên)
        table_name = table_name_map.get(symbol, symbol)

        # Ghi vào PostgreSQL    
        stock.to_sql(
            name=table_name,
            con=engine,
            schema='history_indices', 
            if_exists='replace',
            index=False,
            chunksize=800
        )

        return f"✅ Đã lưu {symbol} vào bảng {table_name}"
    except Exception as e:
        return f"❌ Lỗi khi xử lý {symbol}: {str(e)}"
    
def update_all_stocks(symbol_list):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(get_stock, symbol) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            messages.append(future.result())
    return messages

if __name__ == "__main__":
    print("🚀 Bắt đầu cập nhật dữ liệu...")
    result = update_all_stocks(symbols)
    print("\n📋 Kết quả cập nhật:")
    for line in result:
        print(line)
    print("✅ Đã hoàn tất cập nhật dữ liệu vào cơ sở dữ liệu.")
