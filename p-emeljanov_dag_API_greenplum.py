"""
DAG Получение данных от сервиса по API / трансформация данных / запись в Greenplum
- создание таблицы в GP,
- обращение к API c выводом списка с top3 'location' и генерацией SQL запроса,
- запись данных кортежей в GreenPlum
"""


import logging
import datetime as dt
import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.python import PythonSensor

DEFAULT_ARGS = {
    'owner': 'p_emeljanov',
    'poke_interval': 600
}

with DAG(dag_id="p-emeljanov_dag_lesson5-8",
    schedule_interval='@daily',
    start_date=dt.datetime(2022, 3, 2),
    end_date=dt.datetime(2022, 3, 3),
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['p_emeljanov']
) as dag:


    # Task_1
    start_empty = DummyOperator(task_id="empty")

    # Task_2
    # Создание таблицы  через PostgresOperator # + TODO: Автоинкремент от дублей кортежей
    sql_create_table_ram_location = '''
        CREATE TABLE if not exists public.p_emeljanov_ram_location (
        id serial,                                                  
        "name" varchar,
        "type" varchar,
        dimension varchar,
        resident_cnt varchar        
        );        
    '''
    create_table_ram_location = PostgresOperator(
        task_id='create_table_ram_location',
        postgres_conn_id='conn_greenplum_write',
        sql=sql_create_table_ram_location
    )

    # Task_3
    # Обращение к API c вывод списка top3 'residents', генерация SQL запроса, запись данных кортежей в GreenPlum
    def get_list_top3_location_func():
        """
        Обращение к API c выводом list top3 'location'
        """
        try: # + TODO: обработать исключения сетевых ошибок от API
            URL = 'https://rickandmortyapi.com/api/location'  # адрес API схемы 'location'
            result_get = requests.get(URL)                    # вернет python <class 'requests.models.Response'>
            result_get_json = result_get.json()               # конв. объект 'requests.models.Response' в объект - dict
            logging.info('Ошибок запроса к API НЕТ')
        except:
            logging.info('!!!Ошибка запроса к API!!')
        result_get_json_key1 = result_get_json['results']
        location_list = []
        for location in result_get_json_key1:
            location_dict = {
                'name': location['name'],
                'type': location['type'],
                'dimension': location['dimension'],
                'resident_cnt': len(location['residents'])
            }
            location_list.append(location_dict)
        sorted_locations = sorted(location_list,
                                  key=lambda cnt: cnt['resident_cnt'],
                                  reverse=True)
        top3_location = sorted_locations[:3]

        return top3_location

    def generate_sql_query_func():
        """
        Генерация текста SQL запроса
        """
        insert_values = get_list_top3_location_func()
        if insert_values == None: # + TODO: обработать исключения сетевых ошибок от API
            return logging.info('ИТОГ функции 2(generate_sql_query_insert_top3_location_func): Ошибка!!!, None на входе')
        else:
            insert_values_loc = [f"('{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})"
                         for loc in insert_values]
            # + TODO: Очистка таблицы и сброс счетчика автоинкремента перед вставкой
            insert_sql = f'''
                TRUNCATE TABLE public.p_emeljanov_ram_location RESTART IDENTITY; 
                INSERT INTO public.p_emeljanov_ram_location 
                (name, type, dimension, resident_cnt) VALUES 
                {','.join(insert_values_loc)};
            '''
            return insert_sql

    sql_query_insert_top3_location = generate_sql_query_func()

    # + TODO: обработать исключения сетевых ошибок от API (сенсор на True функции генерации sql запроса)
    sensor_true_generate_sql = PythonSensor(
        task_id='sensor_true_generate_sql',
        python_callable = generate_sql_query_func
    )

    insert_top3_location = PostgresOperator(
        task_id='insert_top3_location',
        postgres_conn_id='conn_greenplum_write',
        sql=sql_query_insert_top3_location
    )

### PIPELINE ###
start_empty >>\
create_table_ram_location >>\
sensor_true_generate_sql >>\
insert_top3_location
