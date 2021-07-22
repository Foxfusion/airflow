from datetime import datetime as dt
from datetime import timedelta
from airflow.utils.dates import days_ago

# The DAG object; we'll need this to instaniate a DAG
from airflow import DAG

#importing the operators required
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

#these args will get passed to each operator
#these can be overridden on a per-task basis during operator
#initalization

# notice the start_date is any date in the past to be able to run it
#as soon as its created

default_args = {
	'owner' : 'airflow',
	'depends_on_past' : False,
	'start_date' : days_ago(2),
	'email' : ['wgfox@foxfusion.net'],
	'email_on_failure' : False,
	'email_on_retry' :  False,
	'retries' : 1,
	'retry_delay' : timedelta(minutes=5)
}

dag = DAG(
 'hell_world',
 description = 'example workflow',
 default_args = default_args,
 schedule_interval = timedelta(days = 1)
 
)


def print_hello();
    return("Hello World")


  #dummy_task_1 and hello_task2 are example s of tasks created by
  #instantiating operators

  #Tasks are generated when instantiating
  #instaniated from an operator is called a constructor. The first
  #aregument task_id acts as a unique identifier for the task 

#AA task must include or inherit the arguments the task_id and owner
#otherwise Airflow will raise an exception


dummy_task_1 = DummOperator(
 task_id = 'dummy_task',
 retries = 0,
 dag = dag)


#setting up dependencies. hello_task_2 will run after succeesful 
#run of dummy_task_1
dummy_task_1 >> hello_task_2


