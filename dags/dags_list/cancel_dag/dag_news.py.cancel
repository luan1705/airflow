from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from pendulum import timezone
import logging

# Import your functions
from utils.new_pipeline import add_postgres, add_bigquery

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Default arguments với cải tiến
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'retries': 5,  # Giảm từ 10 xuống 5
    'retry_delay': timedelta(minutes=30),  # Giảm từ 1h xuống 30 phút
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=2),  # Max delay 2 tiếng
    'execution_timeout': timedelta(hours=3),  # Timeout sau 3 tiếng
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@yourcompany.com'],  # Thay bằng email thực
}

def log_task_start(**context):
    """Log task execution info"""
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    log.info(f"🚀 Starting task: {task_id}")
    log.info(f"📅 Execution date: {execution_date}")
    return f"Task {task_id} started successfully"

def log_task_success(**context):
    """Log successful completion"""
    task_id = context['task_instance'].task_id
    duration = context['task_instance'].duration
    log.info(f"✅ Task {task_id} completed in {duration}s")
    return f"Task {task_id} completed successfully"

def handle_postgres_task(**context):
    """Wrapper cho PostgreSQL task với error handling"""
    try:
        log.info("🐘 Starting PostgreSQL data insertion...")
        result = add_postgres()
        log.info(f"✅ PostgreSQL task completed: {result}")
        
        # Push result to XCom for next task
        context['task_instance'].xcom_push(key='postgres_result', value=result)
        return result
        
    except Exception as e:
        log.error(f"❌ PostgreSQL task failed: {e}")
        # Push failure info to XCom
        context['task_instance'].xcom_push(
            key='postgres_error', 
            value={'error': str(e), 'timestamp': datetime.now().isoformat()}
        )
        raise

def handle_bigquery_task(**context):
    """Wrapper cho BigQuery task với dependency check"""
    try:
        # Check if PostgreSQL task succeeded
        postgres_result = context['task_instance'].xcom_pull(
            task_ids='data_processing.add_postgres',
            key='postgres_result'
        )
        
        if not postgres_result:
            log.warning("⚠️ No data from PostgreSQL task, skipping BigQuery")
            return "Skipped - no data from PostgreSQL"
        
        log.info("☁️ Starting BigQuery data insertion...")
        log.info(f"📊 PostgreSQL result: {postgres_result}")
        
        result = add_bigquery()
        log.info(f"✅ BigQuery task completed: {result}")
        
        # Push result to XCom
        context['task_instance'].xcom_push(key='bigquery_result', value=result)
        return result
        
    except Exception as e:
        log.error(f"❌ BigQuery task failed: {e}")
        context['task_instance'].xcom_push(
            key='bigquery_error', 
            value={'error': str(e), 'timestamp': datetime.now().isoformat()}
        )
        raise

def pipeline_summary(**context):
    """Tạo summary của toàn bộ pipeline"""
    try:
        # Get results from all tasks
        postgres_result = context['task_instance'].xcom_pull(
            task_ids='data_processing.add_postgres',
            key='postgres_result'
        )
        bigquery_result = context['task_instance'].xcom_pull(
            task_ids='data_processing.add_bigquery', 
            key='bigquery_result'
        )
        
        summary = {
            'execution_date': context['execution_date'].isoformat(),
            'postgres_status': 'success' if postgres_result else 'failed',
            'postgres_result': postgres_result,
            'bigquery_status': 'success' if bigquery_result else 'failed', 
            'bigquery_result': bigquery_result,
            'pipeline_status': 'success' if (postgres_result and bigquery_result) else 'partial_failure'
        }
        
        log.info(f"📋 Pipeline Summary: {summary}")
        return summary
        
    except Exception as e:
        log.error(f"❌ Summary task failed: {e}")
        return {'pipeline_status': 'failed', 'error': str(e)}

# DAG Definition
with DAG(
    dag_id="news_pipeline_v2",
    default_args=default_args,
    description="News scraping and data pipeline with error handling",
    start_date=datetime(2025, 9, 5, tzinfo=timezone("Asia/Ho_Chi_Minh")),
    schedule_interval="0 8 * * *",  # Sử dụng schedule_interval thay vì schedule
    catchup=False,  # Đổi thành False để tránh backfill không cần thiết
    max_active_runs=1,  # Chỉ cho phép 1 run cùng lúc
    tags=["news", "ETL", "production"],
    doc_md="""
    ## News Pipeline DAG
    
    This DAG scrapes news data from multiple sources and loads it into PostgreSQL and BigQuery.
    
    ### Tasks:
    1. **PostgreSQL**: Scrape and load data into PostgreSQL
    2. **BigQuery**: Transfer data to BigQuery
    3. **Summary**: Generate pipeline execution summary
    
    ### Schedule: 
    - Runs daily at 8:00 AM Vietnam time
    - Retries up to 5 times with exponential backoff
    - Max execution time: 3 hours
    """,
) as dag:
    
    # Start task
    start_task = DummyOperator(
        task_id="start_pipeline",
        doc_md="Pipeline starting point"
    )
    
    # Pre-check task
    pre_check = PythonOperator(
        task_id="pre_check",
        python_callable=log_task_start,
        doc_md="Log pipeline start information"
    )
    
    # Main data processing tasks trong TaskGroup
    with TaskGroup("data_processing") as data_processing:
        
        add_postgres_task = PythonOperator(
            task_id="add_postgres",
            python_callable=handle_postgres_task,
            pool='postgres_pool',  # Sử dụng pool để limit concurrency
            doc_md="Scrape news data and insert into PostgreSQL",
        )
        
        add_bigquery_task = PythonOperator(
            task_id="add_bigquery", 
            python_callable=handle_bigquery_task,
            pool='bigquery_pool',
            doc_md="Transfer data from PostgreSQL to BigQuery",
        )
        
        # Dependencies trong group
        add_postgres_task >> add_bigquery_task
    
    # Post-processing tasks
    summary_task = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
        trigger_rule='all_done',  # Chạy dù tasks trước đó fail hay không
        doc_md="Generate pipeline execution summary"
    )
    
    success_notification = PythonOperator(
        task_id="success_notification",
        python_callable=log_task_success,
        trigger_rule='all_success',
        doc_md="Log successful pipeline completion"
    )
    
    # End task
    end_task = DummyOperator(
        task_id="end_pipeline",
        trigger_rule='all_done',
        doc_md="Pipeline completion point"
    )
    
    # Task dependencies
    start_task >> pre_check >> data_processing
    data_processing >> summary_task
    data_processing >> success_notification
    [summary_task, success_notification] >> end_task

# Optional: Email notification trên failure
def send_failure_email(**context):
    """Send failure notification email"""
    task_id = context['task_instance'].task_id
    error = context['exception']
    execution_date = context['execution_date']
    
    subject = f"❌ News Pipeline Failed - {execution_date}"
    html_content = f"""
    <h3>Task Failed: {task_id}</h3>
    <p><strong>Execution Date:</strong> {execution_date}</p>
    <p><strong>Error:</strong> {error}</p>
    <p><strong>DAG:</strong> {context['dag'].dag_id}</p>
    """
    
    return EmailOperator(
        task_id='send_failure_email',
        to=['admin@yourcompany.com'],
        subject=subject,
        html_content=html_content,
    )

# Thêm callback function cho DAG
dag.on_failure_callback = send_failure_email