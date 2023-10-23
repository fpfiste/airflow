
import datetime as dt
#from datetime import datetime, timedelta
from textwrap import dedent
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
import logging
import requests
from airflow.models import Connection
import json



ts = dt.datetime.now()
year = str(ts.year)
month = str(ts.month)

nas_con = Connection.get_connection_from_secrets('NAS_FTP')
ts_con = Connection.get_connection_from_secrets('Tankstelle')

#remote_path = f'/Daten/02_Finanzen/A_Rechnungen_Tankstelle/A_Tankstellenabrechnung/{year}/{month}/gas_data.txt'
remote_path = json.loads(nas_con.extra)['tankstellen_file'].format(year, month)
local_path = './gas_data.txt'
data_url = json.loads(ts_con.extra)['data_url'].format(ts_con.host)
close_url = json.loads(ts_con.extra)['close_url'].format(ts_con.host)

def get_file(**kwargs):

    hook = SFTPHook('NAS_FTP')
    
    if hook.path_exists(remote_path):
        hook.retrieve_file(remote_path, local_path, prefetch=True)
        logging.info('File already exists')

    else:
        open(local_path, 'a').close()
        logging.info('File created')

def add_data(**kwargs):
    local_path = kwargs['local_path']
    url = kwargs['url']
    login = kwargs['login']
    pw = kwargs['pw']

    logging.info(url)
    cookies = {
        'lang': 'de',
        'user': login,
        'pw' : pw
        }
    r = requests.get(url, cookies=cookies)
    
    logging.info(r.text)

    with open(local_path, 'r+') as f:
        old_data = f.read().splitlines()
        logging.info(old_data)
        lines = [i for i in r.text.split('\n') if i != '']
        for line in lines[1:-1]:
            if line.strip() in old_data and line.strip() != '':
                logging.info('Line already exists')
            else:
                f.write(line)
                logging.info('Line added: '+line )

def close_positions(**kwargs):
    url = kwargs['url']
    login = kwargs['login']
    pw = kwargs['pw']
    cookies = {
        'lang': 'de',
        'user': login,
        'pw' : pw
        }
    r = requests.post(url, cookies=cookies)

with DAG(
    'gas_station_to_ftp',
    schedule_interval=dt.timedelta(days=1),
    start_date=days_ago(1),
) as dag:


    get_file = PythonOperator(
        task_id='get_file',
        provide_context=True,
        python_callable=get_file,
        op_kwargs={'remote_path': remote_path,'local_path': local_path},
        dag=dag)

    add_data = PythonOperator(
        task_id='add_data',
        provide_context=True,
        python_callable=add_data,
        op_kwargs={'local_path': local_path, 'url':data_url, 'login':ts_con.login, 'pw':ts_con.password},
        dag=dag)


    upload_file = SFTPOperator(
        task_id="upload_file",
        ssh_conn_id="NAS_FTP",
        local_filepath=local_path,
        remote_filepath=remote_path,
        operation="put",
        create_intermediate_dirs=True,
        dag=dag
    ) 

    close_positions = PythonOperator(
        task_id='close_positions',
        provide_context=True,
        python_callable=close_positions,
        op_kwargs={'url': close_url, 'login':ts_con.login, 'pw':ts_con.password},
        dag=dag)

get_file >> add_data >> upload_file >> close_positions

if __name__ == '__main__':
    dag.test()