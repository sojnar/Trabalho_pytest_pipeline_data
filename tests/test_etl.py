from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
from hypothesis import given, strategies as st

from pipeline import etl
from pipeline.etl import extract_validation_metadata, process_minio_object, read_sellin_csv, transform_weekly_top_pdvs

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_FILE = BASE_DIR / 'data' / 'raw' / 'qro_sellin_filtered_5000.csv'


def test_transform_returns_top_10_or_less_per_week() -> None:
    df = read_sellin_csv(RAW_FILE)
    weekly = transform_weekly_top_pdvs(df, top_n=10)

    counts = weekly.groupby('anio_semana').size()
    assert (counts <= 10).all()
    assert weekly['ranking_semana'].min() >= 1


def test_transform_contains_required_columns() -> None:
    df = read_sellin_csv(RAW_FILE)
    weekly = transform_weekly_top_pdvs(df, top_n=10)
    expected = {
        'anio_semana', 'id_pdv', 'venda_total', 'quantidade_total',
        'preco_medio', 'ultimo_dia', 'sku_distintos', 'ranking_semana', 'dt_carga'
    }
    assert expected.issubset(set(weekly.columns))


@given(st.lists(st.decimals(min_value=0, max_value=100000, allow_nan=False, allow_infinity=False), min_size=1, max_size=20))
def test_sales_sum_remains_non_negative(values) -> None:
    df = pd.DataFrame(
        {
            'id_pdv': [1] * len(values),
            'id_sku': [116] * len(values),
            'fecha': ['01/01/2026'] * len(values),
            'anio_semana': [202601] * len(values),
            'cantidad': [1] * len(values),
            'venta': values,
            'precio': [10] * len(values),
        }
    )
    weekly = transform_weekly_top_pdvs(df, top_n=10)
    assert weekly['venda_total'].iloc[0] >= 0


def test_extract_validation_metadata_parses_line_rule_and_column() -> None:
    message = "Arquivo reprovado na validação:\n- linha 1454: venta_must_be_zero_when_cantidad_zero (venta)"
    metadata = extract_validation_metadata(message)
    assert metadata['invalid_line'] == 1454
    assert metadata['invalid_rule'] == 'venta_must_be_zero_when_cantidad_zero'
    assert metadata['invalid_column'] == 'venta'


def test_process_minio_object_returns_error_result_when_validation_fails(monkeypatch) -> None:
    csv_content = (
        'id_pdv;id_sku;fecha_min;fecha_max;anio_semana_min;anio_semana_max;count_nonzero;sum_cantidad;fecha;anio_semana;cantidad;cantidad_neta;cantidad_bruta;venta;venta_neta;venta_bruta;volumen_neto;volumen_bruto;precio\n'
        '1;116;01/01/2026;01/01/2026;202601;202601;0;0;01/01/2026;202601;0;0;0;10;10;10;0;0;10\n'
    ).encode('utf-8')

    response = MagicMock()
    response.read.return_value = csv_content

    client = MagicMock()
    client.get_object.return_value = response

    monkeypatch.setattr(etl, 'get_minio_client', lambda: client)
    monkeypatch.setattr(
        etl,
        'validate_source_file',
        lambda _path: (_ for _ in ()).throw(
            ValueError('Arquivo reprovado na validação:\n- linha 1454: venta_must_be_zero_when_cantidad_zero (venta)')
        ),
    )

    move_mock = MagicMock()
    monkeypatch.setattr(etl, 'move_object_between_buckets', move_mock)

    result = process_minio_object('qro_sellin_filtered_5000.csv', top_n=10)

    assert result.status == 'error'
    assert result.invalid_line == 1454
    assert result.invalid_rule == 'venta_must_be_zero_when_cantidad_zero'
    assert result.invalid_column == 'venta'
    move_mock.assert_called_once()
