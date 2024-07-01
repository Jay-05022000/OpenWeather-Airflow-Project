# import necessary libraries.

from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator
#from airflow.operators.email_operator import EmailOperator
from airflow.operators.email import EmailOperator
from Data_Extraction import Extraction  # Import Extraction function from Data_Extraction.py file.
from Data_Transformation import Transformation  # Import Transformation function from Data_Transformation.py file.

default_arguments = {'owner':'Airflow',                              
                     'start_date':datetime(2024,7,1),
                     'retries':1,
                     'retry_delay':timedelta(seconds = 15)}

with DAG(dag_id='OpenWeather_ETL',default_args = default_arguments,schedule= timedelta(1),template_searchpath=['/home/jay/airflow/Dag_related_files'],catchup=False) as dag:

    T1 = PythonOperator(task_id = 'Data_Extraction_From_API',
                        python_callable = Extraction)
    
    T2 = PythonOperator(task_id = 'Data_Transformation',
                        python_callable = Transformation)
     
    T3 = EmailOperator(task_id = 'send_DAG_run_Status_email',
                    to = 'jaypatel.pt9@gmail.com', # Specify list of email recipients here.
                    subject = 'DAG run status',
                    html_content ='Email_Content.html',
                    conn_id= 'smtp_connection')
                   
                    
    
    T1 >> T2 >> T3  