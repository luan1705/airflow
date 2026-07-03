from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from utils.new_pipeline.merge import tintuc
import pandas as pd
import logging
import time
from datetime import datetime
import traceback

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
)
log = logging.getLogger(__name__)

def create_db_connection(max_retries=3):
    """
    Tạo kết nối database với retry logic
    """
    connection_string = 'postgresql://vnsfintech:%40Vns123456@tanhungsoft.com:5432/vnsfintech'
    
    for attempt in range(max_retries):
        try:
            log.info(f"🔌 Thử kết nối database (lần {attempt + 1}/{max_retries})")
            
            enginedb = create_engine(
                connection_string,
                connect_args={
                    'connect_timeout': 15,
                    'application_name': 'airflow_news_scraper'
                },
                pool_pre_ping=True,  # Verify connections before use
                pool_recycle=3600,   # Recycle connections every hour
                echo=False
            )
            
            # Test connection
            with enginedb.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            
            log.info("✅ Kết nối PostgreSQL thành công")
            return enginedb
            
        except OperationalError as e:
            log.warning(f"⚠️ Kết nối thất bại lần {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                log.info(f"⏳ Chờ {wait_time}s trước khi thử lại...")
                time.sleep(wait_time)
        except Exception as e:
            log.error(f"❌ Lỗi không mong muốn: {e}")
            break
    
    raise ConnectionError("Không thể kết nối database sau nhiều lần thử")

def validate_dataframe(df, required_columns=['Titles', 'Link', 'Source']):
    """
    Validate DataFrame structure và data quality
    """
    if df is None:
        return False, "DataFrame is None"
    
    if df.empty:
        return False, "DataFrame is empty"
    
    # Check required columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        return False, f"Missing columns: {missing_cols}"
    
    # Check for null values in critical columns
    critical_nulls = df[['Titles', 'Link']].isnull().sum()
    if critical_nulls.any():
        log.warning(f"⚠️ Null values found: {critical_nulls.to_dict()}")
        # Remove rows with null Titles or Link
        initial_count = len(df)
        df_clean = df.dropna(subset=['Titles', 'Link'])
        if len(df_clean) != initial_count:
            log.info(f"🧹 Removed {initial_count - len(df_clean)} rows with null Titles/Link")
    
    # Check for empty strings
    empty_titles = (df['Titles'].str.strip() == '').sum()
    empty_links = (df['Link'].str.strip() == '').sum()
    
    if empty_titles > 0 or empty_links > 0:
        log.warning(f"⚠️ Empty strings - Titles: {empty_titles}, Links: {empty_links}")
    
    return True, "Valid DataFrame"

def add_postgres():
    """
    Main function để scrape và lưu vào PostgreSQL
    """
    start_time = datetime.now()
    enginedb = None
    
    try:
        log.info("🚀 Bắt đầu add_postgres task")
        log.info(f"🕒 Start time: {start_time}")
        
        # 1. Lấy dữ liệu từ scrapers
        log.info("📥 Đang lấy dữ liệu từ tintuc()...")
        df = tintuc()
        
        # 2. Validate dữ liệu
        is_valid, message = validate_dataframe(df)
        if not is_valid:
            log.info(f"📭 {message}. Task vẫn SUCCESS.")
            return f"No data: {message}"
        
        log.info(f"📊 Received {len(df)} articles from scrapers")
        log.info(f"📋 Columns: {df.columns.tolist()}")
        
        # 3. Kết nối database
        enginedb = create_db_connection()
        
        # 4. Lấy dữ liệu hiện tại để check trùng lặp
        log.info("🔍 Đang kiểm tra dữ liệu trùng lặp...")
        
        try:
            # Thêm LIMIT để tránh load quá nhiều data nếu bảng lớn
            df_existing = pd.read_sql(
                'SELECT "Titles", "Link" FROM "news"."News" ORDER BY "Date" DESC LIMIT 10000',
                enginedb
            )
            log.info(f"🗃️ Loaded {len(df_existing)} existing records for comparison")
            
        except Exception as e:
            log.warning(f"⚠️ Không thể load existing data: {e}")
            # Nếu bảng không tồn tại hoặc lỗi, coi như không có dữ liệu cũ
            df_existing = pd.DataFrame(columns=['Titles', 'Link'])
        
        # 5. Lọc trùng lặp
        if not df_existing.empty:
            # Merge để tìm records mới
            df_merged = df.merge(
                df_existing, 
                on=['Titles', 'Link'], 
                how='left', 
                indicator=True
            )
            df_new = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])
            
            duplicates_count = len(df) - len(df_new)
            log.info(f"🔍 Found {duplicates_count} duplicates, {len(df_new)} new records")
        else:
            df_new = df
            log.info("🆕 No existing data, all records are new")
        
        # 6. Kiểm tra có dữ liệu mới không
        if df_new.empty:
            log.info("📭 Không có bản ghi mới để thêm vào bảng News.")
            return "Không có bản ghi mới"
        
        # 7. Data cleaning trước khi insert
        log.info("🧹 Cleaning data trước khi insert...")
        
        # Remove duplicates within new data
        initial_new_count = len(df_new)
        df_new = df_new.drop_duplicates(subset=['Titles', 'Link'], keep='first')
        internal_dups = initial_new_count - len(df_new)
        if internal_dups > 0:
            log.info(f"🔄 Removed {internal_dups} internal duplicates")
        
        # Clean strings
        df_new['Titles'] = df_new['Titles'].str.strip()
        df_new['Link'] = df_new['Link'].str.strip()
        
        # 8. Insert vào database
        log.info(f"💾 Đang insert {len(df_new)} records vào database...")
        
        try:
            rows_affected = df_new.to_sql(
                name='News',
                schema='news',
                con=enginedb,
                if_exists='append',
                index=False,
                method='multi'  # Faster batch inserts
            )
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            log.info(f"✅ Successfully inserted {len(df_new)} records vào DB trong {duration:.2f}s")
            
            # Log summary statistics
            if 'Source' in df_new.columns:
                source_stats = df_new['Source'].value_counts().to_dict()
                log.info(f"📊 Records by source: {source_stats}")
            
            return f"Successfully added {len(df_new)} new records to database"
            
        except Exception as e:
            log.error(f"❌ Database insert failed: {e}")
            log.error(f"Chi tiết lỗi: {traceback.format_exc()}")
            raise
    
    except ConnectionError as e:
        log.error(f"❌ Database connection error: {e}")
        raise
    
    except Exception as e:
        log.error(f"❌ Unexpected error in add_postgres: {e}")
        log.error(f"Chi tiết lỗi: {traceback.format_exc()}")
        raise
    
    finally:
        # Cleanup database connection
        if enginedb:
            try:
                enginedb.dispose()
                log.info("🔌 Database connection disposed")
            except:
                pass

