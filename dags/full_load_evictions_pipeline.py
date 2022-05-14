from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

import os
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

GCS_project_id=os.environ["GCP_PROJECT"]
GCS_region=os.environ["COMPOSER_LOCATION"]
composer_bucket = os.environ["GCS_BUCKET"]
GCS_bucket=os.environ["AIRFLOW_VAR_OUTPUT_BUCKET"]

app_token='l0AFei7KQOANOi9RpP3XuO3jo'

with DAG(dag_id='full_load_evictions_pipeline',
        default_args=default_args,
        start_date=datetime(2022,5,4),
        catchup=False) as dag:

    load_eviction_data_to_gcs=BeamRunPythonPipelineOperator(
        task_id="load_eviction_data_to_gcs",
        runner="DataflowRunner",
        py_file=f'gs://{composer_bucket}/dags/dataflows/SodaAPI-GCS.py',
        pipeline_options={
            'tempLocation': f'gs://{GCS_bucket}/temp',
            'stagingLocation': f'gs://{GCS_bucket}/staging',
            'output-bucket': f'gs://{GCS_bucket}/output',
            'api-url':'https://data.cityofnewyork.us/resource/6z8x-wfk4.json',
            'app-token':app_token,
            'api-name':'evictions',
            'execution-date':'{{ds}}'
        },
        py_interpreter='python3',
        py_system_site_packages=False,
        dataflow_config=DataflowConfiguration(
            job_name='{{task.task_id}}', 
            project_id=GCS_project_id,
            location=GCS_region
        ),
    )

    load_census_data_to_gcs=BeamRunPythonPipelineOperator(
        task_id="load_census_data_to_gcs",
        runner="DataflowRunner",
        py_file=f'gs://{composer_bucket}/dags/dataflows/SodaAPI-GCS.py',
        pipeline_options={
            'tempLocation': f'gs://{GCS_bucket}/temp',
            'stagingLocation': f'gs://{GCS_bucket}/staging',
            'output-bucket': f'gs://{GCS_bucket}/output',
            'api-url':'https://data.cityofnewyork.us/resource/rnsn-acs2.json',
            'app-token':app_token,
            'api-name':'census_data',
            'execution-date':'{{ds}}'
        },
        py_options=[],
        py_interpreter='python3',
        py_system_site_packages=False,
        dataflow_config=DataflowConfiguration(
            job_name='{{task.task_id}}', 
            project_id=GCS_project_id,
            location=GCS_region
        )
    )

    create_tables_bq=BigQueryInsertJobOperator(
        task_id='create_tables_bq',
        project_id=GCS_project_id,
        location=GCS_region,
        configuration={
            "query": {
                "query": "{% include 'sql_scripts/create_tables_bq.sql' %}",
                "useLegacySql": False
            }
        }
    )

    copy_eviction_data_to_GCS=GCSToBigQueryOperator(
        task_id='copy_eviction_data_to_GCS',
        bucket=GCS_bucket,
        source_objects=['output/evictions/{{execution_date.year}}/{{execution_date.month}}/{{execution_date.day}}/*'],
        destination_project_dataset_table=f'{GCS_project_id}.eviction_analysis.stg_evictions',
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE'
    )

    copy_census_data_to_GCS=GCSToBigQueryOperator(
        task_id='copy_census_data_to_GCS',
        bucket=GCS_bucket,
        source_objects=['output/census_data/{{execution_date.year}}/{{execution_date.month}}/{{execution_date.day}}/*'],
        destination_project_dataset_table=f'{GCS_project_id}.eviction_analysis.stg_census_data',
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE'
    )

    transform_data_bq = BigQueryInsertJobOperator(
        task_id='transform_data_bq',
        project_id=GCS_project_id,
        location=GCS_region,
        configuration={
            "query": {
                "query": "{% include 'sql_scripts/transform_data_bq_evictions.sql' %}",
                "useLegacySql": False
            }
        }
    )

    [load_eviction_data_to_gcs,load_census_data_to_gcs] >> create_tables_bq
    create_tables_bq >> [copy_eviction_data_to_GCS,copy_census_data_to_GCS]
    [copy_eviction_data_to_GCS,copy_census_data_to_GCS] >>  transform_data_bq