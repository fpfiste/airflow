from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.models import Connection
import logging
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import json




default_args = {
    'owner': 'me',
    'email': ['fabian.pfister@pfister-transporte.ch'],
    'email_on_failure': True,
    'email_on_success': True,
    }



with DAG(
    'collect_backups',
    start_date=days_ago(1),
    schedule_interval='* 3 * * *',
    default_args=default_args
) as dag:

    blob = Connection.get_connection_from_secrets('BlobStorage')

    logging.info(blob.__dict__)

    sr = Connection.get_connection_from_secrets('SR01')

    on_prem_path = json.loads(sr.extra)['sisyphus_backup_path']

    sisyphus_upload_command= f'az storage blob upload-batch --account-name {blob.login} --account-key {blob.password} --destination backups --source {on_prem_path}'
    upload_sisyphus = SSHOperator(
        ssh_conn_id='SR01',
        task_id='upload_sisyphus_backup',
        command=sisyphus_upload_command,
        dag=dag)


    sisyphus_clean_command = f'find {on_prem_path} -mindepth 1 -delete'
    clean_sisyphus = SSHOperator(
        ssh_conn_id='SR01',
        task_id='clean_sisyphus_backup',
        command=sisyphus_clean_command,
        dag=dag)

upload_sisyphus >> clean_sisyphus