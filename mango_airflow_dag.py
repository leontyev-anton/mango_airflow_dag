# credentials нужно вынести в отдельный файл - не доделано
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import requests
import hashlib
import io
import pandas
import pandas_gbq
from google.oauth2 import service_account

vpbx_api_key = '...'   # дано - уникалььный код вашей АТС
vpbx_api_salt = '...'  # дано - ключ для создания подписи
fields = 'records,start,finish,answer,from_extension,from_number,to_extension,to_number,disconnect_reason,entry_id,line_number,location'
period = 'date'
#begin = datetime(2020, 6, 15, hour=0, minute=0, second=0)
#end = datetime(2020, 6, 16, hour=0, minute=0, second=0)
begin = datetime.utcnow() + timedelta(hours=3)
begin = begin - timedelta(days=1)
begin = datetime.strptime(begin.strftime('%Y-%m-%d'),'%Y-%m-%d')
end = begin + timedelta(days=1) - timedelta(seconds=1)  # тк дальше INT Timestamp, поэтому смысла в миллисекундах нет


args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 10, 19),  # hour=11, minute=0, second=0
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'depends_on_past': False,  # если предыдущие запуски (по расписанию походу) сломались, то будет дальше запускаться
    'email_on_failure': True,
    'email_on_retry': False,
    'email': '...',
}

def response1():
    print('response1 start...')
    # формируем параметры для запроса и делаем сам запрос
    begin_str = str(int(begin.timestamp()))
    end_str = str(int(end.timestamp()))
    json = '{"date_from":"' + begin_str + '","date_to":"' + end_str + '","fields":"' + fields + '","request_id":"leontyev_anton_elama"}'
    sign = hashlib.sha256((vpbx_api_key + json + vpbx_api_salt).encode()).hexdigest()
    response1 = requests.post('https://app.mango-office.ru/vpbx/stats/request', data={"vpbx_api_key": vpbx_api_key, "json": json, "sign": sign})  # params = {"vpbx_api_key":vpbx_api_key, "json":json, "sign":sign}

    if response1.status_code == 200:
        print('Requests1 is good. Request2 beginning...')
        return response1.text  # https://airflow.apache.org/docs/stable/concepts.html?highlight=xcom#xcoms
    else:
        print(f'Response1 Status Code = {response1.status_code}')
        print(f'Response1 Headers = {response1.headers}')
        print(f'Response1 Text = {response1.text}')
        raise ValueError('Requests1 is bad. Exit task...')  # Просто Exception не советуют на stack overflow тк...

def response2(**context):
    print('response2 start...')
    json = context['task_instance'].xcom_pull(task_ids='response1')
    sign = hashlib.sha256((vpbx_api_key + json + vpbx_api_salt).encode()).hexdigest()
    #print(json)
    response2 = requests.post('https://app.mango-office.ru/vpbx/stats/result', data={"vpbx_api_key": vpbx_api_key, "json": json, "sign": sign})

    if response2.status_code == 404:
        print(f'Error - Response2 Status Code = {response2.status_code} Not Found, parameter key is wrong.')
        print(f'Response2 Text = {response2.text}')
        raise ValueError('Requests2 has error. Exit task...')
    elif response2.status_code == 204:
        print(f'Response2 Status Code = {response2.status_code}. Data is not ready, you need more attempts.')
        raise ValueError('Exit task...')
    elif response2.status_code == 200:
        context['task_instance'].xcom_push(key='data_string', value=response2.text)  # просто для разнообразия делаю так - без return
    else:
        print(f'Error - Response2 Status Code = {response2.status_code}. Something was wrong, unknown error.')
        print(f'Response2 Text = {response2.text}')
        raise ValueError('Requests2 has error. Exit task...')

def write_to_bigquery(**context):
    print('Begin write_to_bigquery...')
    #print(data_string)
    data_string = io.StringIO(context['task_instance'].xcom_pull(key='data_string'))
    df = pandas.read_csv(data_string, sep=';',names=fields.split(','))
    print(f'Total data rows: {len(df)}. Example (last row): ')
    print(df.iloc[[len(df)-1]])

    if period == 'period':
        table_name='calls_period'
    elif period == 'date':
        table_name = 'calls_' + begin.strftime('%Y%m%d')
    else:
        print('Error with parameter:period in function: write_to_bigquery')
        return
    print(f'Parameter period = {period}')

    # credentials = service_account.Credentials.from_service_account_file('/home/anton/Dropbox/gsc/leontyev-anton.json')
    # try здесь смысла вроде как не имеет, поэтому пишу напрямую
    pandas_gbq.to_gbq(df, 'mango.' + table_name, project_id='...', if_exists='replace')  # если прописывать credentials, то запускать с reauth=True
    print(f'Table in BigQuery mango.{table_name} created success. Number of rows: {len(df)}\n')

with DAG(dag_id='mango_dag', default_args=args, schedule_interval='0 0 * * *') as dag:  # почему-то schedule_interval сверху в args не работает
    response1 = PythonOperator(task_id='response1', python_callable=response1, dag=dag)
    pause = BashOperator(task_id='pause', bash_command='sleep 1', dag=dag)  # не sleep 5 тк оно и так все как-то медленно выполняется
    response2 = PythonOperator(task_id='response2', python_callable=response2, dag=dag, provide_context=True, retries=2,
                               retry_delay=timedelta(seconds=5), trigger_rule='all_success')  #trigger_rule='all_success' - по дефолту
    write_to_bigquery = PythonOperator(task_id='write_to_bigquery', python_callable=write_to_bigquery, dag=dag,
                                       provide_context=True, retries=1, retry_delay=timedelta(minutes=20))

    #response1 >> pause >> response2 >> write_to_bigquery
    pause.set_upstream(response1)
    response2.set_upstream(pause)
    write_to_bigquery.set_upstream(response2)
