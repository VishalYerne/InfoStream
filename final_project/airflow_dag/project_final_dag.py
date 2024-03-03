from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator

# Second approach for creating DAGs
dag1 = DAG(dag_id="project_final_dag",
           schedule_interval='0 0 * * *',
           start_date=datetime(2021, 1, 1),
           catchup=False,
           tags=['final_project', 'capstone_project', 'news_data_lake']
           )
	

taskA = BashOperator(task_id="NewsFetching",
                     bash_command="spark-submit --master local news_fetch.py {{ dag_run.conf['date'] }}",
                     cwd="/home/sidd0613/parquet_based_code",
                     dag=dag1)


taskB2 = BashOperator(task_id="NLPPipeline",
                     bash_command="spark-submit --packages com.johnsnowlabs.nlp:spark-nlp-spark32_2.12:3.4.2 nlp_pipeline.py {{ dag_run.conf['date'] }}",
                     cwd="/home/sidd0613/parquet_based_code",
                     dag=dag1)


taskC = BashOperator(task_id="QueryProcessing",
                     bash_command="spark-submit --master local --jars mysql-connector-java-8.0.26 queries.py {{ dag_run.conf['date'] }}",
                     cwd="/home/sidd0613/parquet_based_code",
                     dag=dag1)


taskA >> taskB2 >> taskC














