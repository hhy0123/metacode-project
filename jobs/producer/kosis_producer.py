import requests
import json
import time
import os
import sys
import boto3
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _runtime import run_daemon

env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)

API_KEY = os.getenv("KOSIS_API_KEY")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
S3_BUCKET = os.getenv("S3_BUCKET")
TOPIC = "kosis-population"
POLL_INTERVAL = int(os.getenv("KOSIS_POLL_INTERVAL", "21600"))  # 기본 6시간

STATS = [
    {"orgId": "101", "tblId": "DT_1B040B3", "name": "시도별가구"},
]

s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

def fetch_kosis(org_id: str, tbl_id: str):
    url = "https://kosis.kr/openapi/Param/statisticsParameterData.do"
    params = {
        "method": "getList",
        "apiKey": API_KEY,
        "itmId": "T1",
        "objL1": "ALL",
        "format": "json",
        "jsonVD": "Y",
        "prdSe": "Y",
        "startPrdDe": "202401",
        "endPrdDe": datetime.now().strftime("%Y%m"),
        "orgId": org_id,
        "tblId": tbl_id,
    }
    res = requests.get(url, params=params, timeout=15)
    res.raise_for_status()
    return res.json()

def parse_and_send(data: list, stat_name: str):
    if not isinstance(data, list):
        return 0
    records = []
    for row in data:
        record = {
            "stat_name": stat_name,
            "ingested_at": datetime.utcnow().isoformat(),
            **row,
        }
        producer.send(TOPIC, value=record)
        records.append(record)
    producer.flush()

    if records:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        key = f"raw/kosis/{today}/{stat_name}.json"
        body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))

    return len(records)

def run_once():
    print(f"[kosis_producer] 폴링 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')} KST)")
    total = 0
    for stat in STATS:
        try:
            data = fetch_kosis(stat["orgId"], stat["tblId"])
            count = parse_and_send(data, stat["name"])
            total += count
            print(f"  [{stat['name']}] {count}건 → Kafka Topic: {TOPIC}")
            time.sleep(1)
        except Exception as e:
            print(f"  [{stat['name']}] 오류: {e}")
    print(f"[kosis_producer] 폴링 완료 — 총 {total}건")
    return total

if __name__ == "__main__":
    run_daemon("kosis_producer", POLL_INTERVAL, run_once)