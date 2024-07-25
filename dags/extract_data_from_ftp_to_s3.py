import pendulum
from airflow.decorators import task, dag
from airflow.operators.python import get_current_context
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.ftp_to_s3 import FTPToS3Operator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator


def extract_from_ftp_to_s3(**kwargs):
    year = kwargs.get('year')
    month = kwargs.get('month')
    month = str(month) if int(month) >= 10 else f'0{month}'

    FTPToS3Operator(
        task_id=f"extract_{year}{month}_files_to_s3",
        aws_conn_id="local_minio",
        ftp_path=f"/pdet/microdados/NOVO CAGED/{year}/{year}{month}",
        ftp_filenames='*',
        s3_bucket='caged',
        s3_key=f'raw/{year}{month}',
        replace=True,
        acl_policy="owner"
    ).execute(dict())

@dag(
    dag_id='extract_data_from_ftp_to_s3',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval="@monthly",
    max_active_runs=1,
    catchup=True
)
def extract_data_from_ftp_to_s3():
    PythonOperator(
        task_id='extract_from_ftp_to_s3',
        python_callable=extract_from_ftp_to_s3,
        op_kwargs={
            'year': '{{ data_interval_start.year }}',
            'month': '{{ data_interval_start.month }}'
        },
    )

dag = extract_data_from_ftp_to_s3()
