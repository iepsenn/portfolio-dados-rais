compose file from [airflow repository](https://github.com/apache/airflow/blob/main/docs/apache-airflow/start/docker-compose.yaml)


### Airflow Connections

#### MinIO
Connection Id: local_minio
Connection Type: S3
Extra: {"aws_access_key_id": "minio_access_key", "aws_secret_access_key": "minio_secret_key", "host": "http://minio:9000"}

#### FTP
Connection Id: ftp_default
Connection Type: SFTP
Host: ftp.mtps.gov.br
Username: anonymous