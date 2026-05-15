"""Bronze 스트리밍 컬럼 매핑이 깨지지 않는지 검증."""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="module")
def consumer_module():
    return importlib.import_module("jobs.consumer.kafka_consumer")


def test_topic_config_covers_three_sources(consumer_module):
    assert set(consumer_module.TOPIC_CONFIG.keys()) == {
        "molit-transactions",
        "rone-price-index",
        "kosis-population",
    }


def test_each_topic_has_checkpoint_and_table(consumer_module):
    for topic, cfg in consumer_module.TOPIC_CONFIG.items():
        assert cfg["table"].startswith("glue_catalog.propberg_bronze.")
        assert cfg["checkpoint"].startswith("s3a://")
        assert topic.replace("-", "_") in cfg["checkpoint"] or topic.split("-")[0] in cfg["checkpoint"]


def test_checkpoint_paths_are_unique(consumer_module):
    """exactly-once 보장 위해 토픽별 checkpoint가 반드시 분리되어야 함."""
    paths = [cfg["checkpoint"] for cfg in consumer_module.TOPIC_CONFIG.values()]
    assert len(set(paths)) == len(paths), "checkpoint paths must be unique per topic"


def test_bronze_columns_mapping_complete(consumer_module):
    """Bronze 테이블 컬럼 목록과 컬럼 매핑이 누락 없이 정의되었는지."""
    for topic in consumer_module.TOPIC_CONFIG:
        assert topic in consumer_module.BRONZE_COLUMNS
        assert topic in consumer_module.COLUMN_MAPPING
        assert topic in consumer_module.SCHEMAS


def test_kafka_offset_columns_present(consumer_module):
    """리플레이/감사를 위해 kafka_partition, kafka_offset, kafka_topic 필수."""
    required = {"kafka_topic", "kafka_partition", "kafka_offset", "ingested_date"}
    for topic, cols in consumer_module.BRONZE_COLUMNS.items():
        missing = required - set(cols)
        assert not missing, f"{topic} missing {missing}"
