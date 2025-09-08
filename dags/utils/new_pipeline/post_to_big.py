from sqlalchemy import create_engine
import pandas as pd
from google.cloud import bigquery
import logging

def add_bigquery():
    log = logging.getLogger(__name__)

    try:
        # Kết nối PostgreSQL
        engine = create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')
        log.info("✅ Kết nối PostgreSQL thành công.")

        # Đọc dữ liệu từ bảng News
        df = pd.read_sql('SELECT * FROM "news"."News"', con=engine)
        if df.empty:
            log.info("📭 Bảng News rỗng, không có dữ liệu để upload lên BigQuery.")
            return "Không có dữ liệu để đẩy lên BigQuery"

        # Kết nối BigQuery bằng service account
        client = bigquery.Client.from_service_account_json('/opt/bigquerrykey/nodal-spot-461206-d5-c9606b21be08.json')

        table_id = 'nodal-spot-461206-d5.News_data.News'  # format: project.dataset.table

        # Cấu hình ghi đè (nếu có bảng cũ)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

        # Tải lên BigQuery
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # Chờ upload xong

        log.info(f"✅ Đã upload {len(df)} dòng lên BigQuery table {table_id}.")

        return f"Đã upload {len(df)} dòng lên BigQuery"

    except Exception as e:
        log.error(f"❌ Lỗi khi đẩy dữ liệu lên BigQuery: {e}")
        raise
