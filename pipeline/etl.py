from __future__ import annotations

import io
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
from minio import Minio
from minio.commonconfig import CopySource
from sqlalchemy import create_engine, text

from pipeline.sellin_validation import split_issues, validate_csv

RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET", "raw")
PROCESSED_BUCKET = os.getenv("MINIO_PROCESSED_BUCKET", "processed")
ERROR_BUCKET = os.getenv("MINIO_ERROR_BUCKET", "erro")
PIPELINE_STATUS_TABLE = os.getenv("PIPELINE_STATUS_TABLE", "pipeline_runs")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sellin")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sellin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sellin")


@dataclass
class PipelineResult:
    source_object: str
    processed_object: str | None
    rows_read: int
    rows_written: int
    status: str
    error_reason: str | None = None
    invalid_line: int | None = None
    invalid_column: str | None = None
    invalid_rule: str | None = None


def get_minio_client() -> Minio:
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
        secure=False,
    )


def get_db_engine():
    return create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )


def get_pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def extract_validation_metadata(error_message: str) -> dict[str, Any]:
    metadata = {"invalid_line": None, "invalid_rule": None, "invalid_column": None}

    line_match = re.search(r"linha\s+(\d+)", error_message, re.IGNORECASE)
    if line_match:
        metadata["invalid_line"] = int(line_match.group(1))

    rule_match = re.search(r":\s*([^\s]+)\s*\(([^)]+)\)", error_message)
    if rule_match:
        metadata["invalid_rule"] = rule_match.group(1).strip()
        metadata["invalid_column"] = rule_match.group(2).strip()

    return metadata


def register_pipeline_run(
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    source_object: str | None,
    target_object: str | None,
    status: str,
    error_reason: str | None = None,
    invalid_line: int | None = None,
    invalid_column: str | None = None,
    invalid_rule: str | None = None,
    rows_read: int | None = None,
    rows_written: int | None = None,
) -> None:
    engine = get_db_engine()
    insert_sql = text(
        f"""
        INSERT INTO {PIPELINE_STATUS_TABLE} (
            dag_id, run_id, task_id, source_object, target_object, status,
            error_reason, invalid_line, invalid_column, invalid_rule,
            rows_read, rows_written
        ) VALUES (
            :dag_id, :run_id, :task_id, :source_object, :target_object, :status,
            :error_reason, :invalid_line, :invalid_column, :invalid_rule,
            :rows_read, :rows_written
        )
        """
    )

    with engine.begin() as conn:
        conn.execute(
            insert_sql,
            {
                "dag_id": dag_id,
                "run_id": run_id,
                "task_id": task_id,
                "source_object": source_object,
                "target_object": target_object,
                "status": status,
                "error_reason": error_reason,
                "invalid_line": invalid_line,
                "invalid_column": invalid_column,
                "invalid_rule": invalid_rule,
                "rows_read": rows_read,
                "rows_written": rows_written,
            },
        )


def move_object_between_buckets(source_bucket: str, source_object: str, target_bucket: str, target_object: str) -> None:
    client = get_minio_client()
    copy_source = CopySource(source_bucket, source_object)
    client.copy_object(target_bucket, target_object, copy_source)
    client.remove_object(source_bucket, source_object)


def remove_object(bucket: str, object_name: str) -> None:
    client = get_minio_client()
    client.remove_object(bucket, object_name)


def validate_source_file(file_path: str | Path) -> None:
    issues = validate_csv(file_path)
    errors, warnings = split_issues(issues)

    if errors:
        grouped = "\n".join(
            f"- linha {issue.row_number}: {issue.code} ({issue.field})"
            for issue in errors[:20]
        )
        raise ValueError(f"Arquivo reprovado na validação:\n{grouped}")

    if warnings:
        print(f"[WARN] arquivo validado com {len(warnings)} warning(s).")


def read_sellin_csv(path_or_buffer) -> pd.DataFrame:
    return pd.read_csv(path_or_buffer, sep=";")


def normalize_sellin_types(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["fecha"] = pd.to_datetime(work["fecha"], format="%d/%m/%Y", errors="coerce")
    work["venta"] = pd.to_numeric(work["venta"], errors="coerce").fillna(0)
    work["cantidad"] = pd.to_numeric(work["cantidad"], errors="coerce").fillna(0)
    work["precio"] = pd.to_numeric(work["precio"], errors="coerce").fillna(0)
    work["anio_semana"] = pd.to_numeric(work["anio_semana"], errors="coerce").fillna(0).astype(int)
    work["id_pdv"] = pd.to_numeric(work["id_pdv"], errors="coerce").fillna(0).astype(int)
    work["id_sku"] = pd.to_numeric(work["id_sku"], errors="coerce").fillna(0).astype(int)
    return work


def transform_weekly_top_pdvs(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    work = normalize_sellin_types(df)
    weekly = (
        work.groupby(["anio_semana", "id_pdv"], as_index=False)
        .agg(
            venda_total=("venta", "sum"),
            quantidade_total=("cantidad", "sum"),
            preco_medio=("precio", "mean"),
            ultimo_dia=("fecha", "max"),
            sku_distintos=("id_sku", "nunique"),
        )
    )
    weekly["ranking_semana"] = weekly.groupby("anio_semana")["venda_total"].rank(method="dense", ascending=False).astype(int)
    weekly = weekly.sort_values(["anio_semana", "ranking_semana", "id_pdv"])
    top = weekly[weekly["ranking_semana"] <= top_n].copy()
    top["dt_carga"] = datetime.now(timezone.utc)
    return top[["anio_semana", "id_pdv", "venda_total", "quantidade_total", "preco_medio", "ultimo_dia", "sku_distintos", "ranking_semana", "dt_carga"]]


def transform_sku_pdv_sales(df: pd.DataFrame) -> pd.DataFrame:
    work = normalize_sellin_types(df)
    sku = (
        work.groupby(["id_pdv", "id_sku"], as_index=False)
        .agg(
            venda_total=("venta", "sum"),
            quantidade_total=("cantidad", "sum"),
            semanas_ativas=("anio_semana", "nunique"),
        )
    )
    sku["dt_carga"] = datetime.now(timezone.utc)
    return sku[["id_pdv", "id_sku", "venda_total", "quantidade_total", "semanas_ativas", "dt_carga"]]


def _prepare_datetime_columns(load_df: pd.DataFrame) -> pd.DataFrame:
    work = load_df.copy()
    for column in work.columns:
        if pd.api.types.is_datetime64_any_dtype(work[column]):
            work[column] = work[column].dt.strftime("%Y-%m-%d %H:%M:%S")
    return work


def copy_dataframe_to_postgres(df: pd.DataFrame, table_name: str, columns: list[str], truncate: bool = True) -> None:
    if df.empty:
        return

    load_df = _prepare_datetime_columns(df[columns])
    conn = get_pg_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                if truncate:
                    cur.execute(f"TRUNCATE TABLE {table_name}")
                buffer = StringIO()
                load_df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)
                cur.copy_expert(
                    f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH CSV",
                    buffer,
                )
    finally:
        conn.close()


def load_weekly_to_postgres(df: pd.DataFrame) -> None:
    copy_dataframe_to_postgres(
        df,
        "weekly_pdv_sales",
        ["anio_semana", "id_pdv", "venda_total", "quantidade_total", "preco_medio", "ultimo_dia", "sku_distintos", "ranking_semana", "dt_carga"],
        truncate=True,
    )


def load_sku_pdv_to_postgres(df: pd.DataFrame) -> None:
    copy_dataframe_to_postgres(
        df,
        "sku_pdv_sales",
        ["id_pdv", "id_sku", "venda_total", "quantidade_total", "semanas_ativas", "dt_carga"],
        truncate=True,
    )


def upload_processed_csv(df: pd.DataFrame, object_name: str) -> str:
    client = get_minio_client()
    csv_bytes = df.to_csv(index=False, sep=";").encode("utf-8")
    client.put_object(PROCESSED_BUCKET, object_name, data=io.BytesIO(csv_bytes), length=len(csv_bytes), content_type="text/csv")
    return object_name


def process_minio_object(object_name: str, top_n: int = 10) -> PipelineResult:
    if not object_name:
        raise ValueError("object_name não foi informado para process_minio_object")

    client = get_minio_client()
    response = client.get_object(RAW_BUCKET, object_name)
    try:
        data = response.read()
    finally:
        response.close()
        response.release_conn()

    df = read_sellin_csv(io.BytesIO(data))
    rows_read = len(df)
    temp_file = Path("/tmp") / Path(object_name).name
    temp_file.write_bytes(data)

    try:
        validate_source_file(temp_file)
        weekly = transform_weekly_top_pdvs(df, top_n=top_n)
        sku_pdv = transform_sku_pdv_sales(df)
        load_weekly_to_postgres(weekly)
        load_sku_pdv_to_postgres(sku_pdv)

        processed_name = f"weekly_top_pdvs_{Path(object_name).stem}.csv"
        sku_processed_name = f"sku_pdv_sales_{Path(object_name).stem}.csv"
        upload_processed_csv(weekly, processed_name)
        upload_processed_csv(sku_pdv, sku_processed_name)
        remove_object(RAW_BUCKET, object_name)

        return PipelineResult(
            source_object=object_name,
            processed_object=f"{processed_name}, {sku_processed_name}",
            rows_read=rows_read,
            rows_written=len(weekly) + len(sku_pdv),
            status="success",
        )

    except ValueError as exc:
        error_message = str(exc)
        metadata = extract_validation_metadata(error_message)
        error_object_name = f"erro/{os.path.basename(object_name)}"
        move_object_between_buckets(RAW_BUCKET, object_name, ERROR_BUCKET, error_object_name)
        return PipelineResult(
            source_object=object_name,
            processed_object=error_object_name,
            rows_read=rows_read,
            rows_written=0,
            status="error",
            error_reason=error_message,
            invalid_line=metadata["invalid_line"],
            invalid_column=metadata["invalid_column"],
            invalid_rule=metadata["invalid_rule"],
        )


def list_candidate_objects(prefix: str = "") -> list[str]:
    client = get_minio_client()
    objects = client.list_objects(RAW_BUCKET, prefix=prefix, recursive=True)
    return sorted([obj.object_name for obj in objects if obj.object_name.endswith(".csv")])
