import psycopg2
import pandas as pd

# Kết nối database
DB_URL = "postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl"

def extract_filename_from_url(pdf_link):
    """Lấy tên file từ URL (phần sau dấu / cuối cùng)"""
    if pdf_link and pdf_link != '[null]' and pd.notna(pdf_link):
        return pdf_link.split('/')[-1]
    return None

def get_data_from_tables():
    """Lấy dữ liệu từ 2 bảng link và test - CHỈ LẤY upload_status = 'done'"""
    
    try:
        conn = psycopg2.connect(DB_URL)
        
        # Query kết hợp 2 bảng documents.link và documents.test
        # CHỈ LẤY upload_status = 'done'
        query = """
        SELECT 
            l.symbol,
            t.news_title as title,
            t.news_short_content as content,
            t.public_date as date,
            l.pdf_link,
            l.news_source_link as source
        FROM documents.link l
        INNER JOIN documents.test t ON l.id = t.id
        WHERE l.pdf_link IS NOT NULL 
          AND l.pdf_link != '[null]'
          AND l.upload_status = 'done'
        ORDER BY l.symbol, l.id ASC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Tạo fileName từ pdf_link
        df['fileName'] = df['pdf_link'].apply(extract_filename_from_url)
        
        # Loại bỏ cột pdf_link
        df = df.drop('pdf_link', axis=1)
        
        # Sắp xếp lại các cột
        df = df[['symbol', 'title', 'content', 'date', 'fileName', 'source']]
        
        return df
        
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def create_tables_in_db(df):
    """Tạo các bảng trong schema pdf"""
    
    if df is None or df.empty:
        print("Không có dữ liệu")
        return False
    
    try:
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        
        # Tạo schema nếu chưa tồn tại
        cur.execute("CREATE SCHEMA IF NOT EXISTS pdf;")
        
        symbols = sorted(df['symbol'].unique())
        
        for symbol in symbols:
            # Tên bảng viết HOA
            table_name = symbol.upper()
            
            # Tạo bảng cho mỗi symbol
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS pdf."{table_name}" (
                title TEXT,
                content TEXT,
                date TIMESTAMP,
                fileName TEXT,
                source TEXT
            );
            """
            cur.execute(create_table_sql)
            
            # Tạo indexes
            cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON pdf."{table_name}"(date);')
            cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_fileName ON pdf."{table_name}"(fileName);')
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✓ Đã tạo {len(symbols)} bảng trong schema pdf")
        return True
        
    except Exception as e:
        print(f"Lỗi khi tạo bảng: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def insert_data_to_db(df):
    """Insert dữ liệu vào các bảng trong schema pdf"""
    
    if df is None or df.empty:
        print("Không có dữ liệu để insert")
        return False
    
    try:
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        
        symbols = sorted(df['symbol'].unique())
        total_inserted = 0
        
        for symbol in symbols:
            df_symbol = df[df['symbol'] == symbol].copy()
            
            # Tên bảng viết HOA
            table_name = symbol.upper()
            
            # Xóa dữ liệu cũ trong bảng trước khi insert
            cur.execute(f'TRUNCATE TABLE pdf."{table_name}";')
            
            inserted = 0
            for _, row in df_symbol.iterrows():
                insert_sql = f"""
                INSERT INTO pdf."{table_name}" (title, content, date, fileName, source)
                VALUES (%s, %s, %s, %s, %s)
                """
                
                cur.execute(insert_sql, (
                    row['title'] if pd.notna(row['title']) else None,
                    row['content'] if pd.notna(row['content']) else None,
                    row['date'] if pd.notna(row['date']) else None,
                    row['fileName'] if pd.notna(row['fileName']) else None,
                    row['source'] if pd.notna(row['source']) else None
                ))
                inserted += 1
            
            print(f"  - Bảng pdf.{table_name}: {inserted} bản ghi")
            total_inserted += inserted
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"\n✓ Tổng cộng đã insert {total_inserted} bản ghi vào {len(symbols)} bảng")
        return True
        
    except Exception as e:
        print(f"Lỗi khi insert dữ liệu: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Hàm chính"""
    
    print("="*80)
    print("CHƯƠNG TRÌNH LƯU DỮ LIỆU VÀO DATABASE (CHỈ UPLOAD_STATUS = 'DONE')")
    print("="*80)
    
    # Bước 1: Lấy dữ liệu
    print("\n1. Đang lấy dữ liệu từ database (chỉ upload_status = 'done')...")
    df = get_data_from_tables()
    
    if df is not None and not df.empty:
        print(f"✓ Đã lấy được {len(df)} bản ghi (chỉ upload_status = 'done')")
        print(f"✓ Số lượng symbols: {df['symbol'].nunique()}")
        
        # Bước 2: Tạo bảng
        print("\n2. Đang tạo các bảng trong schema pdf...")
        if create_tables_in_db(df):
            
            # Bước 3: Insert dữ liệu
            print("\n3. Đang insert dữ liệu vào các bảng...")
            if insert_data_to_db(df):
                print("\n" + "="*80)
                print("HOÀN THÀNH!")
                print("="*80)
            else:
                print("\n✗ Có lỗi khi insert dữ liệu")
        else:
            print("\n✗ Có lỗi khi tạo bảng")
    else:
        print("✗ Không có dữ liệu (hoặc không có bản ghi nào upload_status = 'done')")

if __name__ == "__main__":
    main()