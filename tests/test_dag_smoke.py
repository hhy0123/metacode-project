"""Airflow DAG 파일이 import 에러 없이 로드되는지 (구조 검증)."""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.mark.parametrize("dag_file", list(DAGS_DIR.glob("*.py")))
def test_dag_file_parses(dag_file: Path) -> None:
    source = dag_file.read_text(encoding="utf-8")
    # airflow가 없는 환경에서도 syntax만은 검증
    ast.parse(source)


def test_pipeline_dag_has_bronze_freshness_sensor() -> None:
    """스트리밍 적재가 살아있는지 배치 시작 전에 검증."""
    src = (DAGS_DIR / "pipeline_dag.py").read_text(encoding="utf-8")
    assert "bronze_freshness_check" in src
    assert "PythonSensor" in src


def test_mgmt_dag_has_four_operations() -> None:
    """rewrite_manifests → compaction → expire → orphan 순서 보장."""
    src = (DAGS_DIR / "mgmt_dag.py").read_text(encoding="utf-8")
    for op in ("rewrite_manifests", "compaction", "expire_snapshots", "remove_orphans"):
        assert op in src
    # 순서 검증
    idx = [src.index(op) for op in ("rewrite_manifests", "compaction", "expire_snapshots", "remove_orphans")]
    assert idx == sorted(idx), "mgmt DAG operations must be in correct order"
