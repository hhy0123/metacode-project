"""데이터 품질·일관성 검증 — 강사 피드백 반영(AI 검증 레이어).

이 테스트는 코드와 문서가 약속한 invariant를 직접 검증한다. Glue/Spark 의존성
없이 정적으로 검증 가능한 것만 본다 (CI 환경 가벼움).

검증 invariant:
1. README/문서에 적힌 "Bronze는 Iceberg, ingested_date 파티셔닝, kafka 메타 보존"이
   실제 DDL에 반영되어 있는가
2. silver_job이 약속한 `streaming_health` 적재 함수를 진짜 구현하는가
   (docs에 적혀 있는데 코드에 없으면 fail)
3. kafka_consumer가 ingested_date를 KST로 처리하는가 (UTC 잔재 없는가)
4. 모든 DAG의 schedule이 timezone-aware로 정의되어 있는가
5. mgmt_job이 Bronze 3개 + Silver 3개 + Gold 3개 = 9개 테이블을 모두 다루는가
"""
from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


# --- invariant 1: Bronze DDL이 Iceberg + 파티셔닝 + kafka 메타 ---
def test_bronze_ddl_has_iceberg_and_kafka_meta() -> None:
    sql = (ROOT / "ddl" / "bronze.sql").read_text(encoding="utf-8")
    assert sql.count("'table_type'='ICEBERG'") == 3, "Bronze 3개 모두 ICEBERG여야 함"
    assert sql.count("PARTITIONED BY (ingested_date)") == 3, "Bronze 3개 모두 ingested_date 파티셔닝"
    for c in ("kafka_topic", "kafka_partition", "kafka_offset"):
        assert sql.count(c) >= 3, f"{c} 컬럼이 Bronze 3개 모두에 있어야 함"


# --- invariant 2: silver_job이 streaming_health 적재 함수를 구현 ---
def test_silver_job_implements_streaming_health() -> None:
    src = (ROOT / "jobs" / "silver" / "silver_job.py").read_text(encoding="utf-8")
    assert "def process_streaming_health" in src, (
        "docs/architecture.md, observability.md에서 약속한 streaming_health 적재 함수가 코드에 없음"
    )
    assert "MERGE INTO glue_catalog.propberg_gold.streaming_health" in src, (
        "streaming_health 테이블에 실제 MERGE INTO 호출이 있어야 함"
    )
    assert "process_streaming_health()" in src, (
        "__main__ 블록에서 streaming_health도 호출되어야 함"
    )


# --- invariant 3: kafka_consumer가 ingested_date를 KST로 ---
def test_kafka_consumer_uses_kst_for_partition_date() -> None:
    """ingested_date 파티션 컬럼이 KST 기준이어야 한다.
    UTC로 두면 KST 자정 부근 row가 전날 파티션으로 들어가 파티션 프루닝 깨짐 — 강사 피드백."""
    src = (ROOT / "jobs" / "consumer" / "kafka_consumer.py").read_text(encoding="utf-8")
    # 1) KST timezone offset 정의
    assert "timedelta(hours=9)" in src, "KST timezone offset (timedelta(hours=9)) 정의 필요"
    # 2) today 변수를 KST 기반으로 생성
    assert "datetime.now(KST)" in src, (
        "today 변수를 datetime.now(KST).strftime(...) 으로 생성해야 함"
    )
    # 3) today가 ingested_date로 들어가는지
    assert 'withColumn("ingested_date", F.lit(today))' in src, (
        "ingested_date 컬럼 값은 KST 기반 today여야 함"
    )
    # 4) UTC strftime이 ingested_date 부근에 남아있으면 안 됨
    assert "datetime.utcnow()" not in src, "datetime.utcnow() 잔재 — KST로 통일 필요"


# --- invariant 4: 모든 DAG가 timezone-aware schedule ---
@pytest.mark.parametrize("dag_file", ["mgmt_dag.py", "pipeline_dag.py"])
def test_dag_has_timezone_aware_start_date(dag_file: str) -> None:
    src = (ROOT / "dags" / dag_file).read_text(encoding="utf-8")
    assert 'pendulum.datetime(' in src and 'tz="Asia/Seoul"' in src, (
        f"{dag_file}: start_date가 pendulum.datetime(..., tz='Asia/Seoul')로 timezone-aware해야 함"
    )


# --- invariant 5: mgmt_job이 9개 테이블 모두 다룸 ---
def test_mgmt_job_covers_all_9_tables() -> None:
    src = (ROOT / "jobs" / "mgmt_job.py").read_text(encoding="utf-8")
    expected = [
        "propberg_bronze.raw_transactions",
        "propberg_bronze.raw_price_index",
        "propberg_bronze.raw_population",
        "propberg_silver.transactions_enriched",
        "propberg_silver.price_index_enriched",
        "propberg_silver.population_enriched",
        "propberg_gold.daily_trade_summary",
        "propberg_gold.monthly_price_trend",
        "propberg_gold.anomaly_transactions",
    ]
    for t in expected:
        assert t in src, f"mgmt_job이 {t} 테이블을 다루지 않음"


# --- invariant 6: README에 약속한 파일들이 실제로 존재 ---
def test_promised_artifacts_exist() -> None:
    promised = [
        "jobs/consumer/kafka_consumer.py",
        "jobs/silver/silver_job.py",
        "jobs/gold/gold_job.py",
        "jobs/mgmt_job.py",
        "jobs/producer/_runtime.py",
        "jobs/producer/molit_producer.py",
        "jobs/producer/rone_producer.py",
        "jobs/producer/kosis_producer.py",
        "dags/pipeline_dag.py",
        "dags/mgmt_dag.py",
        "ddl/bronze.sql",
        "ddl/silver.sql",
        "ddl/gold.sql",
        "docs/architecture.md",
        "docs/observability.md",
        "infra/docker/docker-compose.yml",
        "infra/docker/Dockerfile.producer",
        "infra/docker/Dockerfile.streaming",
        "infra/docker/prometheus.yml",
        "monitoring/grafana/dashboards/propberg_streaming.json",
        ".github/workflows/ci.yml",
        ".env.example",
        "pyproject.toml",
        ".pre-commit-config.yaml",
        "README.md",
    ]
    missing = [p for p in promised if not (ROOT / p).exists()]
    assert not missing, f"README/문서에 적혔지만 실제로 없는 파일: {missing}"
