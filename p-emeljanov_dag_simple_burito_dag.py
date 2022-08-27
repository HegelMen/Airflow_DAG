"""
DAG выборка данных из таблицы GreenPlum и вывод их в лог
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook
import random
import logging
import datetime as dt

DEFAULT_ARGS = {
    'owner': 'p_emeljanov',
    'poke_interval': 600
}

with DAG(dag_id="p-emeljanov_dag_lesson3-4",
    schedule_interval='@daily',                    # расписание реализовал с ShortCircuitOperator
    start_date=dt.datetime(2022, 3, 2),
    end_date=dt.datetime(2022, 3, 15),
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['p_emeljanov']
) as dag:


# Таск_1 DummyOperator
# Таск_2 ShortCircuitOperator
    def is_work_weekend_func(execution_dt):
        """
        Планировщик DAG'a по рабочим дням
        """
        exec_day = dt.datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day in [0,1,2,3,4,5]

    work_weekend = ShortCircuitOperator(
        task_id='work_weekend',
        python_callable=is_work_weekend_func,
        op_kwargs={'execution_dt': '{{ds}}'}
    )

# Таск_3 BashOperator
    task_echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo Дата сегодня: {{ ds }} и день недели: {{ execution_date}}'
    )

# Таск_4 PythonOperator
    def hello_world_func():
        logging.info("First message: Hello World!")

    task_msg_hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func
    )

# Таск_5 BranchPythonOperator + PythonOperator
    """
    Random выбор + таски ВЕТВЛЕНИЯ_простая логика. Вывод в лог.
    """
    def city_route_func(*op_args):
        target_city = random.choice(op_args)
        logging.info(f'Second message: Random city to discovery {target_city}')
        if target_city == 'Sochi':
            return 'task_route_sochi'
        elif target_city == 'Madrid':
            return 'task_route_madrid'
        else:
            return 'task_route_london'

    task_discovery_route = BranchPythonOperator(
        task_id='discovery_route',
        python_callable=city_route_func,
        op_args = ['Sochi', 'Madrid', 'London']
    )

    def task_route_sochi_func():                          # Таск_5_1 task_route_sochi
        logging.info("sochi message: task_route_sochi")

    task_route_sochi = PythonOperator(
        task_id='task_route_sochi',
        python_callable=task_route_sochi_func
    )

    def task_route_madrid_func():                         # Таск_5_2 task_route_madrid
        logging.info("madrid message: task_route_madrid")

    task_route_madrid = PythonOperator(
        task_id='task_route_madrid',
        python_callable=task_route_madrid_func
    )

    def task_route_london_func():                         # Таск_5_3 task_route_london
        logging.info("london message: task_route_london")

    task_route_london = PythonOperator(
        task_id='task_route_london',
        python_callable=task_route_london_func
    )


#Таск_6
    def select_from_gp_func(**kwargs):
        """
        Подключение к GP и выборка данных, вывод инф. в консоль
        """
        this_date = kwargs['ds']                                          # получаем str дату из контекста по ключу
        this_date_date = dt.datetime.strptime(this_date, '%Y-%m-%d')      # переводим str дату в date
        dw = this_date_date.weekday() + 1                                 # метод datetime- день недели dw (Python начинает с 0)
        logging.info(f'Дата {this_date_date} День недели: {dw}')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')         # инициализируем хук
        conn = pg_hook.get_conn()                                         # создаем объект соединения
        cursor = conn.cursor()                                            # открываем курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {dw}')   # выполняем sql с параметром дня недели dw
        query_res = cursor.fetchall()                                     # присваиваем имя данным из курсора
        conn.close()
        logging.info(f'Результат запроса к GreenPlum: {query_res}')       # выводим в консоль c распаковкой

    task_select_from_gp = PythonOperator(
        task_id='task_select_from_gp',
        python_callable=select_from_gp_func,
        trigger_rule='one_success',
        op_kwargs = {'ds'},
        provide_context = True
    )

### PIPELINE ###
work_weekend >> [task_echo_ds, task_msg_hello_world] >> \
task_discovery_route >> [task_route_sochi, task_route_madrid, task_route_london] >>\
task_select_from_gp