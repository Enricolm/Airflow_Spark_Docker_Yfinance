from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from os.path import join
from pathlib import Path
from airflow.models import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator 

with DAG (dag_id='Investimento_api', start_date=datetime.now(), schedule_interval='0 0 * * 2-6') as dag:

    Spark_extracao_dados = SparkSubmitOperator(
        task_id = 'Extracao_Invest',
        conn_id = 'spark-submit',
        application = join(str(Path('../')),
                        ('operators/Raw_Operator.py')),
        name = 'Extracao_Invest'
    )

    PostgresOperator = PostgresOperator(
        task_id = 'Insert_Postgres',
        sql = f"""COPY Apple_table FROM '{join(Path('~/Documents').expanduser(),f'Sprinklr_Airflow/dadosvm/Airflow_Investimento/datalake/Raw/Data={datetime.now().strftime("%Y-%m-%d")}')}' CSV HEADER;""",
        postgres_conn_id ='',
        database = 'Apple_datatable',
        autocommit=True
    )


Spark_extracao_dados >> PostgresOperator