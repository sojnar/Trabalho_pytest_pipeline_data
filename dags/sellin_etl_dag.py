from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException

from pipeline.etl import list_candidate_objects, process_minio_object, register_pipeline_run

DAG_ID = 'sellin_minio_to_grafana'
RAW_PREFIX = os.getenv('RAW_PREFIX', '')
TOP_N = int(os.getenv('TOP_N_PDVS', '10'))

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 1, 1),
    schedule='*/2 * * * *',
    catchup=False,
    tags=['minio', 'etl', 'tests', 'grafana'],
    max_active_runs=1,
) as dag:

    @task
    def discover_latest_file() -> str:
        objects = list_candidate_objects(prefix=RAW_PREFIX)
        if not objects:
            raise AirflowSkipException('Nenhum CSV encontrado no bucket raw.')
        return objects[-1]

    @task
    def run_pipeline(object_name: str, dag_run=None, task_instance=None) -> dict:
        if not object_name:
            raise AirflowFailException('run_pipeline recebeu object_name vazio.')

        result = process_minio_object(object_name, top_n=TOP_N)

        register_pipeline_run(
            dag_id=DAG_ID,
            run_id=dag_run.run_id if dag_run else 'manual',
            task_id='run_pipeline',
            source_object=result.source_object,
            target_object=result.processed_object,
            status=result.status,
            error_reason=result.error_reason,
            invalid_line=result.invalid_line,
            invalid_column=result.invalid_column,
            invalid_rule=result.invalid_rule,
            rows_read=result.rows_read,
            rows_written=result.rows_written,
        )

        if result.status == 'error':
            raise AirflowFailException(
                f'Falha de qualidade no arquivo {result.source_object}. '
                f'Arquivo movido para {result.processed_object}. '
                f'Linha inválida: {result.invalid_line}. '
                f'Regra: {result.invalid_rule}. '
                f'Coluna: {result.invalid_column}.'
            )

        return result.__dict__

    run_pipeline(discover_latest_file())
