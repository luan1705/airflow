from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ================== DEFAULT ARGS ================== #
default_args = {
    'owner': 'qviet',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ================== DAG DEFINITION ================== #
with DAG(
    dag_id='code_cung_sequential',
    default_args=default_args,
    description='Run all Python scripts sequentially in code_cung folder',
    schedule_interval='0 1 * * *',  # Chạy lúc 1h sáng mỗi ngày
    start_date=datetime(2026, 1, 14),
    catchup=False,
    tags=['code_cung', 'sequential'],
) as dag:

    base_path = '/www/server/airflow/dags/code_cung'

    # ================== TASKS (CHẠY TUẦN TỰ) ================== #
    
    task_1_company = BashOperator(
        task_id='1_run_company',
        bash_command=f'cd {base_path} && python company.py',
    )

    task_2_cw = BashOperator(
        task_id='2_run_cw',
        bash_command=f'cd {base_path} && python cw.py',
    )

    task_3_derivative = BashOperator(
        task_id='3_run_derivative',
        bash_command=f'cd {base_path} && python derivative.py',
    )

    task_4_eps = BashOperator(
        task_id='4_run_eps',
        bash_command=f'cd {base_path} && python eps.py',
    )

    task_5_industry = BashOperator(
        task_id='5_run_industry',
        bash_command=f'cd {base_path} && python industry.py',
    )

    task_6_pe_pb = BashOperator(
        task_id='6_run_pe_pb',
        bash_command=f'cd {base_path} && python pe_pb.py',
    )

    task_7_weight = BashOperator(
        task_id='7_run_weight',
        bash_command=f'cd {base_path} && python weight.py',
    )