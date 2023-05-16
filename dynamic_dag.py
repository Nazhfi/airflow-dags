import subprocess
from datetime import datetime, timedelta

import pendulum
from airflow.api.common.experimental.delete_dag import delete_dag
from airflow.models import DAG, DagModel, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.date_time import DateTimeSensor
from airflow.utils.email import send_email_smtp
from airflow.utils.session import provide_session


DYNAMIC_TAG = "dynamic_d1_dummy"

local_tz = pendulum.timezone("Asia/Jakarta")
enabled_dags = []

number_of_dags = Variable.get("dag_number", default_var=1)
number_of_dags = int(number_of_dags)

for n in range(1, number_of_dags):
    default_args = {
        "owner": 'Cloud',
        "start_date": datetime(2023, 1, 1, tzinfo=local_tz),
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }

    dag_id = "d1_dynamic_dag_pipeline{}".format(str(n))
    dag = DAG(
        dag_id,
        default_args=default_args,
        schedule_interval="0 7 * * *",
        tags=['dynamic_d1_dummy'],
        catchup=False,
    )

    with dag:
        task = BashOperator(
            task_id='t1',
            bash_command='echo "Hello World!" && sleep 10',
            dag=dag
        )

    globals()[dag_id] = dag
    enabled_dags.append(dag_id)


@provide_session
def get_all_dag_ids(session=None):
    all_objs = session.query(DagModel).all()
    return [i.dag_id for i in all_objs if DYNAMIC_TAG in i.tags and ENV in i.tags]


all_dag_ids = get_all_dag_ids()  # all dag in database
dag_ids_to_del = [dag_id for dag_id in all_dag_ids if dag_id not in enabled_dags]

for k in dag_ids_to_del:
    delete_dag(k)
    del globals()[k]
