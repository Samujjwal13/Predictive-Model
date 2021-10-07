"""
python module import
"""
from datetime import timedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator  import PythonOperator
from selfcuremodel.selfcure import selfcure_main
from selfcuremodel.email_utility.email_utility import SendEmail
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}
def task_failure_email_alert(context):
    """
    Send custom email alerts when job failled
    """
    job_name = "Training"
    task_id = context['task_instance'].task_id
    execution_date = context['task_instance'].execution_date
    execution_date = str(execution_date)+" UTC"
    try_number = context['task_instance']._try_number
    max_tries = context['task_instance'].max_tries
    hostname = context['task_instance'].hostname
    log_url = context['task_instance'].log_url
    trigger_type = "";
    if context.get('dag_run').external_trigger:
        trigger_type = "Manual Run"
    else:
        trigger_type = "Scheduled Run"
    task_Owner = context['task'].owner
    SendEmail.task_failure_email_alert(job_name, task_id, execution_date, try_number, max_tries, hostname, log_url, task_Owner, trigger_type)
    return 1
def task_success_email_alert(context):
    """
    Send custom email alerts when job success
    """
    job_name = "Training"
    task_id = context['task_instance'].task_id
    execution_date = context['task_instance'].execution_date
    execution_date = str(execution_date)+" UTC"
    try_number = context['task_instance']._try_number
    max_tries = context['task_instance'].max_tries
    hostname = context['task_instance'].hostname
    log_url = context['task_instance'].log_url
    trigger_type = "";
    if context.get('dag_run').external_trigger:
        trigger_type = "Manual Run"
    else:
        trigger_type = "Scheduled Run"
    task_Owner = context['task'].owner
    SendEmail.task_success_email_alert(job_name, task_id, execution_date, try_number, max_tries, hostname, log_url, task_Owner, trigger_type)
    return 1
def collect_param(**kwargs):
    """
    This function takes / collects all parameter from airflow dag's 
    operator and passes to selfcure module selfcure_main function .
    this function collects only jon name from the operator 
    """
    if kwargs["test_mode"]:
        print("Running under Test Mode")
    else:
        print("Running under Normal / Airflow Dag Mode")
    job_name = kwargs["params"]["job_Name"]
    selfcure_main(job_name)
    print("Exiting  Collect_All_Param funtion call")
    return 1
PREDICTIVE_MODEL_TRAINING_DAG = DAG(
    dag_id='Selfcure_Training',
    default_args=DEFAULT_ARGS,
    #schedule_interval='44 10 * * *',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
)
VALIDATETRAININGDATA = PythonOperator(
    task_id='ValidateTraining',
    provide_context=True,
    python_callable=collect_param,
    on_failure_callback=task_failure_email_alert,
    params={"job_Name":"ValidateTraining"},
    dag=PREDICTIVE_MODEL_TRAINING_DAG,
)
FEATUREENGINEERING = PythonOperator(
    task_id='FeatureEngineering',
    provide_context=True,
    python_callable=collect_param,
    on_failure_callback=task_failure_email_alert,
    params={"job_Name":"FeatureEngineering"},
    dag=PREDICTIVE_MODEL_TRAINING_DAG,
)
TRAININGTHEMODEL = PythonOperator(
    task_id='TrainingTheModel',
    provide_context=True,
    python_callable=collect_param,
    on_failure_callback=task_failure_email_alert,
    on_success_callback=task_success_email_alert,
    params={"job_Name":"ModelTrainiing"},
    dag=PREDICTIVE_MODEL_TRAINING_DAG,
)
# [run PreProcessingOfTraining first and then TrainingTheModel in sequence]
VALIDATETRAININGDATA >> FEATUREENGINEERING >> TRAININGTHEMODEL
# Entry point for Python Application
if __name__ == '__main__':
    PREDICTIVE_MODEL_TRAINING_DAG.cli()
    
