import requests
import json
import time
import os
import boto3
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)

API_KEY = os.getenv("RONE_API_KEY")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
S3_BUCKET = os.getenv("S3_BUCKET")
TOPIC = "rone-price-index"
STATBL_ID = "A_2024_00045"

print(f"[debug] RONE_API_KEY: {API_KEY[:6]}..." if API_KEY else "[debug] API_KEY 없음")

s3 = boto3.client("s3", region_name="ap-northeast-2")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

YEAR_MONTHS = [
    f"{year}{month:02d}"
    for year in range(2024, 2026)
    for month in range(1, 13)
]

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

def parse_records(data: dict, year_month: str):
    try:
        rows = data["SttsApiTblData"][1]["row"]
    except (KeyError, IndexError):
        return []
    records = []
    for row in rows:
        record = {
            "year_month": year_month,
            "ingested_at": datetime.utcnow().isoformat(),
            **row,
        }
        records.append(record)
    return records

def save_to_s3(records: list, year_month: str):
    if not records:
        return
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key = f"raw/rone/{today}/{year_month}.json"
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))

def run():
    total = 0
    for year_month in YEAR_MONTHS:
        try:
            data = fetch_price_index(year_month)
            records = parse_records(data, year_month)
            for r in records:
                producer.send(TOPIC, value=r)
            producer.flush()
            save_to_s3(records, year_month)
            total += len(records)
            if records:
                print(f"  [{year_month}] {len(records)}건 전송+S3저장")
            time.sleep(0.5)
        except Exception as e:
            print(f"  [{year_month}] 오류: {e}")
    print(f"[rone_producer] 완료 — 총 {total}건")

if __name__ == "__main__":
    print("[rone_producer] 시작")
    run()