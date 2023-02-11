import pendulum
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.transfers.ftp_to_s3 import FTPToS3Operator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator

months = [
    str(number)
    if number >= 10
    else f'0{number}'
    for number in range(1, 13)
]


@dag(
    dag_id='extract_data_from_ftp_to_s3',
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    schedule_interval="@yearly",
    max_active_runs=1,
    catchup=False
)
def extract_data_from_ftp_to_s3():
    year = '{{ data_interval_start.year }}'

    create_bucket = S3CreateBucketOperator(
        aws_conn_id='local_minio',
        task_id="create_bucket",
        bucket_name='caged'
    )

    for month in months[:1]:
        ftp_to_s3_task = FTPToS3Operator(
            task_id=f"extract_{year}{month}_files_to_s3",
            aws_conn_id="local_minio",
            ftp_path=f"/pdet/microdados/NOVO CAGED/{year}/{year}{month}",
            ftp_filenames='*',
            s3_bucket='caged',
            s3_key=f'raw/{year}{month}/',
            replace=True,
            acl_policy="owner"
        )
        create_bucket >> ftp_to_s3_task


dag = extract_data_from_ftp_to_s3()
