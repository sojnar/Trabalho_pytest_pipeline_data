CREATE TABLE IF NOT EXISTS weekly_pdv_sales (
    anio_semana INTEGER,
    id_pdv BIGINT,
    venda_total NUMERIC,
    quantidade_total NUMERIC,
    preco_medio NUMERIC,
    ultimo_dia TIMESTAMP,
    sku_distintos INTEGER,
    ranking_semana INTEGER,
    dt_carga TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sku_pdv_sales (
    id_pdv BIGINT,
    id_sku BIGINT,
    venda_total NUMERIC,
    quantidade_total NUMERIC,
    semanas_ativas INTEGER,
    dt_carga TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id BIGSERIAL PRIMARY KEY,
    dag_id VARCHAR(255) NOT NULL,
    run_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    source_object VARCHAR(1024),
    target_object VARCHAR(1024),
    status VARCHAR(32) NOT NULL,
    error_reason TEXT,
    invalid_line INTEGER,
    invalid_column VARCHAR(255),
    invalid_rule VARCHAR(255),
    rows_read INTEGER,
    rows_written INTEGER,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_processed_at ON pipeline_runs (processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_weekly_pdv_sales_id_pdv ON weekly_pdv_sales (id_pdv);
CREATE INDEX IF NOT EXISTS idx_sku_pdv_sales_id_pdv ON sku_pdv_sales (id_pdv);
