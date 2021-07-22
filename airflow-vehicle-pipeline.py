from dateutil import parser
from datetime import datetime, timedelta
from datetime import timezone
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from cloudera.cdp.airflow.operators.cdw_operator import CDWOperator


default_args = {
    'owner': 'csso_sahmadian',
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime(2021,1,1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    'vehicle-pipeline',
    default_args=default_args, 
    schedule_interval='0 0 * * *', 
    catchup=False, 
    is_paused_upon_creation=False
)


start = DummyOperator(task_id='start', dag=dag)


customer_data = CDEJobRunOperator(
    task_id='customer_data',
    retries=3,
    dag=dag,
    job_name='customer-MDM'
)

parts_data = CDEJobRunOperator(
    task_id='parts_data',
    dag=dag,
    job_name='VehicleParts'
)

repairs_data = CDEJobRunOperator(
    task_id='repairs_data',
    dag=dag,
    job_name='CertifiedRepairs-SAP'
)

forecast_maintenance = CDEJobRunOperator(
    task_id='forecast-maintenance',
    dag=dag,
    job_name='forecast-maintenance'
)

insurance_claims = CDEJobRunOperator(
    task_id='insurance-claims',
    dag=dag,
    job_name='InsuranceClaims'
)

end = DummyOperator(task_id='end', dag=dag)


start >> customer_data >> [parts_data, repairs_data] >>  end
repairs_data >> forecast_maintenance
start >> insurance_claims >> end
