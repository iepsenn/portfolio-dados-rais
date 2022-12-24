from __future__ import annotations

from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.ftp.hooks.ftp import FTPHook

if TYPE_CHECKING:
    from airflow.utils.context import Context



from airflow.providers.amazon.aws.transfers.ftp_to_s3 import FTPToS3Operator


class FTPTransfer(FTPToS3Operator):
    def __init__(
        self,
        *,
        ftp_path: str,
        s3_bucket: str,
        s3_key: str,
        ftp_filenames: str | list[str] | None = None,
        s3_filenames: str | list[str] | None = None,
        ftp_conn_id: str = 'ftp_default',
        aws_conn_id: str = 'aws_default',
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: str | None = None,
        **kwargs,
    ):
        super().__init__(
            ftp_path,
            s3_bucket,
            s3_key,
            ftp_filenames,
            s3_filenames,
            ftp_conn_id,
            aws_conn_id,
            replace,
            encrypt,
            gzip,
            acl_policy,
            **kwargs,    
        )

    def __upload_to_s3_from_ftp(self, remote_filename, s3_file_key):
        with NamedTemporaryFile() as local_tmp_file:
            self.ftp_hook.retrieve_file(
                remote_full_path=remote_filename, local_full_path_or_buffer=local_tmp_file.name
            )

            self.s3_hook.load_file(
                filename=local_tmp_file.name,
                key=s3_file_key,
                bucket_name=self.s3_bucket,
                replace=self.replace,
                encrypt=self.encrypt,
                gzip=self.gzip,
                acl_policy=self.acl_policy,
            )
            self.log.info('File upload to %s', s3_file_key)

    def execute(self, context: Context):
        self.ftp_hook = FTPHook(ftp_conn_id=self.ftp_conn_id)
        self.s3_hook = S3Hook(self.aws_conn_id)

        if self.ftp_filenames:
            if isinstance(self.ftp_filenames, str):
                self.log.info('Getting files in %s', self.ftp_path)

                list_dir = self.ftp_hook.list_directory(
                    path=self.ftp_path,
                )

                if self.ftp_filenames == '*':
                    files = list_dir
                else:
                    ftp_filename: str = self.ftp_filenames
                    files = list(filter(lambda f: ftp_filename in f, list_dir))

                for file in files:
                    self.log.info('Moving file %s', file)

                    if self.s3_filenames and isinstance(self.s3_filenames, str):
                        filename = file.replace(self.ftp_filenames, self.s3_filenames)
                    else:
                        filename = file

                    s3_file_key = f'{self.s3_key}{filename}'
                    self.__upload_to_s3_from_ftp(file, s3_file_key)

            else:
                if self.s3_filenames:
                    for ftp_file, s3_file in zip(self.ftp_filenames, self.s3_filenames):
                        self.__upload_to_s3_from_ftp(self.ftp_path + ftp_file, self.s3_key + s3_file)
                else:
                    for ftp_file in self.ftp_filenames:
                        self.__upload_to_s3_from_ftp(self.ftp_path + ftp_file, self.s3_key + ftp_file)
        else:
            self.__upload_to_s3_from_ftp(self.ftp_path, self.s3_key)