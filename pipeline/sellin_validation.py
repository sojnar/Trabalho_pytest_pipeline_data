from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

EXPECTED_COLUMNS = [
    "id_pdv",
    "id_sku",
    "fecha_min",
    "fecha_max",
    "anio_semana_min",
    "anio_semana_max",
    "count_nonzero",
    "sum_cantidad",
    "fecha",
    "anio_semana",
    "cantidad",
    "cantidad_neta",
    "cantidad_bruta",
    "venta",
    "venta_neta",
    "venta_bruta",
    "volumen_neto",
    "volumen_bruto",
    "precio",
]

INTEGER_FIELDS = {
    "id_pdv",
    "id_sku",
    "anio_semana_min",
    "anio_semana_max",
    "count_nonzero",
    "sum_cantidad",
    "anio_semana",
    "cantidad",
    "cantidad_neta",
    "cantidad_bruta",
}

DECIMAL_FIELDS = {
    "venta",
    "venta_neta",
    "venta_bruta",
    "volumen_neto",
    "volumen_bruto",
    "precio",
}

DATE_FIELDS = {"fecha_min", "fecha_max", "fecha"}

NON_NEGATIVE_FIELDS = {
    "id_pdv",
    "id_sku",
    "anio_semana_min",
    "anio_semana_max",
    "count_nonzero",
    "sum_cantidad",
    "anio_semana",
    "cantidad",
    "cantidad_bruta",
    "venta",
    "venta_bruta",
    "volumen_bruto",
    "precio",
}

GROUP_METADATA_FIELDS = [
    "fecha_min",
    "fecha_max",
    "anio_semana_min",
    "anio_semana_max",
    "count_nonzero",
    "sum_cantidad",
]

SKU_VOLUME_FACTORS = {
    116: Decimal("0.250"),
    294: Decimal("0.500"),
    384: Decimal("0.450"),
    542: Decimal("0.250"),
    975: Decimal("0.266"),
    2741: Decimal("0.250"),
    2749: Decimal("0.500"),
    4464: Decimal("0.432"),
}

TWO_DECIMALS = Decimal("0.01")
THREE_DECIMALS = Decimal("0.001")


@dataclass(frozen=True)
class Issue:
    severity: str
    code: str
    message: str
    row_number: int | None = None
    field: str | None = None


def _round_money(value: Decimal) -> Decimal:
    return value.quantize(TWO_DECIMALS, rounding=ROUND_HALF_UP)


def _round_volume(value: Decimal) -> Decimal:
    return value.quantize(THREE_DECIMALS, rounding=ROUND_HALF_UP)


def _is_blank(value: object) -> bool:
    return value is None or str(value).strip() == ""


def _parse_integer(value: str, field: str, row_number: int, issues: list[Issue]) -> int | None:
    if _is_blank(value):
        issues.append(
            Issue(
                severity="error",
                code="blank_required_field",
                message=f"O campo {field} é obrigatório.",
                row_number=row_number,
                field=field,
            )
        )
        return None

    try:
        parsed = Decimal(str(value))
    except InvalidOperation:
        issues.append(
            Issue(
                severity="error",
                code="invalid_integer",
                message=f"O campo {field} precisa ser inteiro.",
                row_number=row_number,
                field=field,
            )
        )
        return None

    if parsed != parsed.to_integral_value():
        issues.append(
            Issue(
                severity="error",
                code="invalid_integer",
                message=f"O campo {field} precisa ser inteiro.",
                row_number=row_number,
                field=field,
            )
        )
        return None

    return int(parsed)


def _parse_decimal(value: str, field: str, row_number: int, issues: list[Issue]) -> Decimal | None:
    if _is_blank(value):
        issues.append(
            Issue(
                severity="error",
                code="blank_required_field",
                message=f"O campo {field} é obrigatório.",
                row_number=row_number,
                field=field,
            )
        )
        return None

    try:
        return Decimal(str(value))
    except InvalidOperation:
        issues.append(
            Issue(
                severity="error",
                code="invalid_decimal",
                message=f"O campo {field} precisa ser numérico.",
                row_number=row_number,
                field=field,
            )
        )
        return None


def _parse_date(value: str, field: str, row_number: int, issues: list[Issue]) -> datetime | None:
    if _is_blank(value):
        issues.append(
            Issue(
                severity="error",
                code="blank_required_field",
                message=f"O campo {field} é obrigatório.",
                row_number=row_number,
                field=field,
            )
        )
        return None

    try:
        return datetime.strptime(value, "%d/%m/%Y")
    except ValueError:
        issues.append(
            Issue(
                severity="error",
                code="invalid_date_format",
                message=f"O campo {field} precisa seguir o formato DD/MM/YYYY.",
                row_number=row_number,
                field=field,
            )
        )
        return None


def _validate_columns(fieldnames: list[str] | None) -> list[Issue]:
    issues: list[Issue] = []
    incoming = fieldnames or []
    missing = [column for column in EXPECTED_COLUMNS if column not in incoming]
    extra = [column for column in incoming if column not in EXPECTED_COLUMNS]

    for column in missing:
        issues.append(
            Issue(
                severity="error",
                code="missing_column",
                message=f"A coluna obrigatória {column} não existe no arquivo.",
                field=column,
            )
        )

    for column in extra:
        issues.append(
            Issue(
                severity="warning",
                code="unexpected_column",
                message=f"A coluna {column} não faz parte do contrato atual do pipeline.",
                field=column,
            )
        )

    return issues


def validate_rows(
    rows: list[dict[str, str]],
    *,
    strict_group_aggregates: bool = False,
) -> list[Issue]:
    issues: list[Issue] = []
    seen_keys: set[tuple[str, str, str]] = set()
    groups: dict[tuple[str, str], dict[str, object]] = {}

    for row_number, row in enumerate(rows, start=1):
        parsed_ints: dict[str, int | None] = {}
        parsed_decimals: dict[str, Decimal | None] = {}
        parsed_dates: dict[str, datetime | None] = {}

        for field in INTEGER_FIELDS:
            parsed_ints[field] = _parse_integer(row.get(field, ""), field, row_number, issues)

        for field in DECIMAL_FIELDS:
            parsed_decimals[field] = _parse_decimal(row.get(field, ""), field, row_number, issues)

        for field in DATE_FIELDS:
            parsed_dates[field] = _parse_date(row.get(field, ""), field, row_number, issues)

        for field in NON_NEGATIVE_FIELDS:
            if field in INTEGER_FIELDS:
                value = parsed_ints[field]
            else:
                value = parsed_decimals[field]
            if value is not None and value < 0:
                issues.append(
                    Issue(
                        severity="error",
                        code="negative_gross_metric",
                        message=f"O campo {field} não pode ser negativo.",
                        row_number=row_number,
                        field=field,
                    )
                )

        id_pdv = row.get("id_pdv", "")
        id_sku = row.get("id_sku", "")
        fecha = row.get("fecha", "")
        if not _is_blank(id_pdv) and not _is_blank(id_sku) and not _is_blank(fecha):
            key = (id_pdv, id_sku, fecha)
            if key in seen_keys:
                issues.append(
                    Issue(
                        severity="error",
                        code="duplicate_business_key",
                        message="A chave (id_pdv, id_sku, fecha) deve ser única.",
                        row_number=row_number,
                    )
                )
            else:
                seen_keys.add(key)

        fecha_value = parsed_dates["fecha"]
        if fecha_value is not None and parsed_ints["anio_semana"] is not None:
            iso_year, iso_week, _ = fecha_value.isocalendar()
            expected_year_week = iso_year * 100 + iso_week
            if parsed_ints["anio_semana"] != expected_year_week:
                issues.append(
                    Issue(
                        severity="error",
                        code="anio_semana_mismatch",
                        message="anio_semana não bate com a semana ISO calculada a partir de fecha.",
                        row_number=row_number,
                        field="anio_semana",
                    )
                )

        cantidad = parsed_ints["cantidad"]
        cantidad_neta = parsed_ints["cantidad_neta"]
        cantidad_bruta = parsed_ints["cantidad_bruta"]
        venta = parsed_decimals["venta"]
        venta_neta = parsed_decimals["venta_neta"]
        venta_bruta = parsed_decimals["venta_bruta"]
        volumen_neto = parsed_decimals["volumen_neto"]
        volumen_bruto = parsed_decimals["volumen_bruto"]
        precio = parsed_decimals["precio"]
        sku = parsed_ints["id_sku"]

        if cantidad is not None and cantidad_bruta is not None and cantidad_bruta != cantidad:
            issues.append(
                Issue(
                    severity="error",
                    code="cantidad_bruta_must_equal_cantidad",
                    message="Na base Sell-In analisada, cantidad_bruta deve ser igual a cantidad.",
                    row_number=row_number,
                    field="cantidad_bruta",
                )
            )

        if venta is not None and venta_bruta is not None and _round_money(venta_bruta) != _round_money(venta):
            issues.append(
                Issue(
                    severity="error",
                    code="venta_bruta_must_equal_venta",
                    message="Na base Sell-In analisada, venta_bruta deve ser igual a venta.",
                    row_number=row_number,
                    field="venta_bruta",
                )
            )

        if cantidad is not None and precio is not None:
            if cantidad > 0 and precio <= 0:
                issues.append(
                    Issue(
                        severity="error",
                        code="precio_must_be_positive_when_cantidad_positive",
                        message="Quando cantidad > 0, precio precisa ser positivo.",
                        row_number=row_number,
                        field="precio",
                    )
                )
            if cantidad == 0 and precio != 0:
                issues.append(
                    Issue(
                        severity="error",
                        code="precio_must_be_zero_when_cantidad_zero",
                        message="Quando cantidad = 0, precio deve ser zero.",
                        row_number=row_number,
                        field="precio",
                    )
                )

        if cantidad is not None and precio is not None and venta is not None:
            if cantidad > 0 and _round_money(Decimal(cantidad) * precio) != _round_money(venta):
                issues.append(
                    Issue(
                        severity="error",
                        code="venta_formula_mismatch",
                        message="venta deve ser igual a cantidad * precio.",
                        row_number=row_number,
                        field="venta",
                    )
                )
            if cantidad == 0 and _round_money(venta) != Decimal("0.00"):
                issues.append(
                    Issue(
                        severity="error",
                        code="venta_must_be_zero_when_cantidad_zero",
                        message="Quando cantidad = 0, venta deve ser zero.",
                        row_number=row_number,
                        field="venta",
                    )
                )

        if cantidad_bruta is not None and precio is not None and venta_bruta is not None:
            if cantidad_bruta > 0 and _round_money(Decimal(cantidad_bruta) * precio) != _round_money(venta_bruta):
                issues.append(
                    Issue(
                        severity="error",
                        code="venta_bruta_formula_mismatch",
                        message="venta_bruta deve ser igual a cantidad_bruta * precio.",
                        row_number=row_number,
                        field="venta_bruta",
                    )
                )
            if cantidad_bruta == 0 and _round_money(venta_bruta) != Decimal("0.00"):
                issues.append(
                    Issue(
                        severity="error",
                        code="venta_bruta_must_be_zero_when_cantidad_zero",
                        message="Quando cantidad_bruta = 0, venta_bruta deve ser zero.",
                        row_number=row_number,
                        field="venta_bruta",
                    )
                )

        factor = SKU_VOLUME_FACTORS.get(sku) if sku is not None else None
        if sku is not None and factor is None:
            issues.append(
                Issue(
                    severity="warning",
                    code="unknown_sku_volume_factor",
                    message=f"O SKU {sku} não possui fator de volume cadastrado no validador.",
                    row_number=row_number,
                    field="id_sku",
                )
            )

        if factor is not None and cantidad_bruta is not None and volumen_bruto is not None:
            expected = _round_volume(Decimal(cantidad_bruta) * factor)
            if _round_volume(volumen_bruto) != expected:
                issues.append(
                    Issue(
                        severity="error",
                        code="volumen_bruto_formula_mismatch",
                        message="volumen_bruto deve ser igual a cantidad_bruta * fator_volume_sku.",
                        row_number=row_number,
                        field="volumen_bruto",
                    )
                )

        if factor is not None and cantidad_neta is not None and volumen_neto is not None:
            expected = _round_volume(Decimal(cantidad_neta) * factor)
            if _round_volume(volumen_neto) != expected:
                issues.append(
                    Issue(
                        severity="error",
                        code="volumen_neto_formula_mismatch",
                        message="volumen_neto deve ser igual a cantidad_neta * fator_volume_sku.",
                        row_number=row_number,
                        field="volumen_neto",
                    )
                )

        fecha_min = parsed_dates["fecha_min"]
        fecha_max = parsed_dates["fecha_max"]
        if fecha_value is not None and fecha_min is not None and fecha_max is not None:
            if not (fecha_min <= fecha_value <= fecha_max):
                issues.append(
                    Issue(
                        severity="warning",
                        code="fecha_outside_header_range",
                        message="fecha ficou fora do intervalo informado por fecha_min/fecha_max.",
                        row_number=row_number,
                        field="fecha",
                    )
                )

        anio_semana = parsed_ints["anio_semana"]
        anio_semana_min = parsed_ints["anio_semana_min"]
        anio_semana_max = parsed_ints["anio_semana_max"]
        if (
            anio_semana is not None
            and anio_semana_min is not None
            and anio_semana_max is not None
            and not (anio_semana_min <= anio_semana <= anio_semana_max)
        ):
            issues.append(
                Issue(
                    severity="warning",
                    code="anio_semana_outside_header_range",
                    message="anio_semana ficou fora do intervalo informado por anio_semana_min/anio_semana_max.",
                    row_number=row_number,
                    field="anio_semana",
                )
            )

        if cantidad_neta is not None and cantidad_bruta is not None and cantidad_neta > cantidad_bruta:
            issues.append(
                Issue(
                    severity="warning",
                    code="cantidad_neta_exceeds_bruta",
                    message="cantidad_neta ficou acima de cantidad_bruta; tratar como ajuste e não como erro duro.",
                    row_number=row_number,
                    field="cantidad_neta",
                )
            )

        if cantidad_neta is not None and venta_neta is not None:
            if cantidad_neta > 0 and venta_neta < 0:
                issues.append(
                    Issue(
                        severity="warning",
                        code="positive_quantity_negative_net_sale",
                        message="Há quantidade líquida positiva com venda líquida negativa.",
                        row_number=row_number,
                        field="venta_neta",
                    )
                )
            if cantidad_neta < 0 and venta_neta > 0:
                issues.append(
                    Issue(
                        severity="warning",
                        code="negative_quantity_positive_net_sale",
                        message="Há quantidade líquida negativa com venda líquida positiva.",
                        row_number=row_number,
                        field="venta_neta",
                    )
                )

        group_key = (row.get("id_pdv", ""), row.get("id_sku", ""))
        if not _is_blank(group_key[0]) and not _is_blank(group_key[1]):
            group_state = groups.setdefault(
                group_key,
                {
                    "first_row": row_number,
                    "metadata": {field: row.get(field, "") for field in GROUP_METADATA_FIELDS},
                    "rows": [],
                },
            )
            for field in GROUP_METADATA_FIELDS:
                if row.get(field, "") != group_state["metadata"][field]:
                    issues.append(
                        Issue(
                            severity="error",
                            code="group_metadata_inconsistent",
                            message=f"O campo {field} precisa ser constante dentro do par (id_pdv, id_sku).",
                            row_number=row_number,
                            field=field,
                        )
                    )
            group_state["rows"].append((row_number, row, parsed_ints))

    if strict_group_aggregates:
        for group_state in groups.values():
            header_count = group_state["metadata"]["count_nonzero"]
            header_sum = group_state["metadata"]["sum_cantidad"]
            first_row = int(group_state["first_row"])
            count_nonzero = 0
            sum_cantidad = 0
            for _, _, parsed_ints in group_state["rows"]:
                cantidad = parsed_ints["cantidad"]
                if cantidad is None:
                    continue
                if cantidad > 0:
                    count_nonzero += 1
                sum_cantidad += cantidad
            if header_count != "" and int(header_count) != count_nonzero:
                issues.append(
                    Issue(
                        severity="warning",
                        code="group_aggregate_mismatch",
                        message="count_nonzero do header não bate com a soma calculada no arquivo carregado.",
                        row_number=first_row,
                        field="count_nonzero",
                    )
                )
            if header_sum != "" and int(header_sum) != sum_cantidad:
                issues.append(
                    Issue(
                        severity="warning",
                        code="group_aggregate_mismatch",
                        message="sum_cantidad do header não bate com a soma calculada no arquivo carregado.",
                        row_number=first_row,
                        field="sum_cantidad",
                    )
                )

    return issues


def validate_csv(path: str | Path, *, strict_group_aggregates: bool = False) -> list[Issue]:
    csv_path = Path(path)
    with csv_path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        issues = _validate_columns(reader.fieldnames)
        rows = list(reader)
    issues.extend(validate_rows(rows, strict_group_aggregates=strict_group_aggregates))
    return issues


def split_issues(issues: list[Issue]) -> tuple[list[Issue], list[Issue]]:
    errors = [issue for issue in issues if issue.severity == "error"]
    warnings = [issue for issue in issues if issue.severity == "warning"]
    return errors, warnings
