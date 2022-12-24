from email import policy
import pendulum
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.transfers.ftp_to_s3 import FTPToS3Operator
from airflow.providers.amazon.aws.transfers.local_to_s3 import  LocalFilesystemToS3Operator

@dag(
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="@hourly",
    max_active_runs=1,
    catchup=False
)
def extract_data_from_ftp_to_s3():
    ftp_to_s3_task = FTPToS3Operator(
        task_id="ftp_to_s3_task",
        aws_conn_id="local_minio",
        ftp_path="/pdet/microdados/NOVO CAGED/2020/202002/",
        s3_bucket='caged',
        s3_key='raw/2022B',
        replace=True,
        acl_policy="owner"
    )

#def LocalToS3Operator():
#    local_to_s3_task =  LocalFilesystemToS3Operator(
#        task_id="local_to_s3_task",
#        aws_conn_id="s3_conn",
#        filename = "/opt/airflow/test.txt",
#        dest_bucket='minio',
#        dest_key='caged/a.7z',
#        replace=True,
#        acl_policy="owner"
#    )

dag = extract_data_from_ftp_to_s3()
#dag = LocalToS3Operator()