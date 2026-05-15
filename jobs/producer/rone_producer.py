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

API_KEY = os.getenv("RONE_API_KEY")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
S3_BUCKET = os.getenv("S3_BUCKET")
TOPIC = "rone-price-index"
STATBL_ID = "A_2024_00045"
POLL_INTERVAL = int(os.getenv("RONE_POLL_INTERVAL", "3600"))

s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

def fetch_price_index(year_month: str):
    url = "https://www.reb.or.kr/r-one/openapi/SttsApiTblData.do"
    params = {
        "KEY": API_KEY,
        "STATBL_ID": STATBL_ID,
        "DTACYCLE_CD": "MM",
        "WRTTIME_IDTFR_ID": year_month,
        "Type": "json",
        "pIndex": 1,
        "pSize": 300,
    }
    res = requests.get(url, params=params, timeout=10)
    res.raise_for_status()
    return res.json()

def parse_and_send(data: dict, year_month: str):
    try:
        rows = data["SttsApiTblData"][1]["row"]
    except (KeyError, IndexError):
        return 0
    records = []
    for row in rows:
        record = {
            "year_month": year_month,
            "ingested_at": datetime.utcnow().isoformat(),
            **row,
        }
        producer.send(TOPIC, value=record)
        records.append(record)
    producer.flush()

    if records:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        key = f"raw/rone/{today}/{year_month}.json"
        body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))

    return len(records)

def run_once():
    year_month = datetime.now().strftime("%Y%m")
    print(f"[rone_producer] {year_month} 폴링 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')} KST)")
    try:
        data = fetch_price_index(year_month)
        count = parse_and_send(data, year_month)
        print(f"[rone_producer] 폴링 완료 — {count}건 → Kafka Topic: {TOPIC}")
        return count
    except Exception as e:
        print(f"[rone_producer] 오류: {e}")
        return 0

if __name__ == "__main__":
    run_daemon("rone_producer", POLL_INTERVAL, run_once)