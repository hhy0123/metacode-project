"""DDL 파일이 Iceberg + 파티셔닝 + 매니지먼트 요건을 만족하는지 검증."""
from __future__ import annotations

from pathlib import Path

import pytest

DDL_DIR = Path(__file__).resolve().parents[1] / "ddl"


@pytest.fixture(scope="module")
def all_ddl() -> dict[str, str]:
    return {p.stem: p.read_text(encoding="utf-8") for p in DDL_DIR.glob("*.sql")}


@pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
def test_all_tables_are_iceberg(all_ddl: dict[str, str], layer: str) -> None:
    sql = all_ddl[layer]
    create_count = sql.count("CREATE TABLE")
    iceberg_count = sql.count("'table_type'='ICEBERG'")
    assert create_count == iceberg_count, (
        f"{layer}: CREATE TABLE={create_count} but ICEBERG={iceberg_count}"
    )


def test_bronze_has_kafka_offset_columns(all_ddl: dict[str, str]) -> None:
    sql = all_ddl["bronze"]
    assert "kafka_topic" in sql
    assert "kafka_partition" in sql
    assert "kafka_offset" in sql


def test_bronze_partitioned_by_ingested_date(all_ddl: dict[str, str]) -> None:
    sql = all_ddl["bronze"]
    assert sql.count("PARTITIONED BY (ingested_date)") >= 3


def test_silver_transactions_partitioned_for_merge(all_ddl: dict[str, str]) -> None:
    """Silver MERGE INTO 성능을 위해 sido/deal_year 파티셔닝 필수."""
    sql = all_ddl["silver"]
    assert "PARTITIONED BY (sido, deal_year)" in sql


def test_target_file_size_configured(all_ddl: dict[str, str]) -> None:
    """소파일 폭주 방지: 128MB target 명시."""
    for layer, sql in all_ddl.items():
        if "PARTITIONED BY" in sql:
            assert "134217728" in sql, f"{layer} missing target-file-size"
