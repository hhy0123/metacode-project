"""DDL 파일이 Iceberg + 파티셔닝 + 매니지먼트 요건을 만족하는지 검증."""
from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DDL_DIR = ROOT / "ddl"


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


def test_iceberg_vacuum_policy_in_ddl(all_ddl: dict[str, str]) -> None:
    """모든 DDL에 Iceberg snapshot retention 정책이 명시되어야 함.
    vacuum_max_snapshot_age_seconds = 604800 (7일), vacuum_min_snapshots_to_keep = 3.
    """
    for layer, sql in all_ddl.items():
        assert "vacuum_max_snapshot_age_seconds" in sql, (
            f"{layer}: vacuum_max_snapshot_age_seconds 정책 누락"
        )
        assert "vacuum_min_snapshots_to_keep" in sql, (
            f"{layer}: vacuum_min_snapshots_to_keep 정책 누락"
        )


def test_mgmt_compaction_target_128mb() -> None:
    """compaction은 mgmt_job.py에서 호출되며 target-file-size-bytes=134217728(128MB)이어야 함.
    DDL의 write.target-file-size-bytes는 Athena 호환성을 위해 제거되었고
    compaction 시점에만 적용되도록 mgmt_job의 rewrite_data_files 호출에서 명시한다.
    """
    src = (ROOT / "jobs" / "mgmt_job.py").read_text(encoding="utf-8")
    assert "'target-file-size-bytes'" in src and "'134217728'" in src, (
        "mgmt_job의 rewrite_data_files 호출에서 target-file-size-bytes=134217728(128MB) 필요"
    )
