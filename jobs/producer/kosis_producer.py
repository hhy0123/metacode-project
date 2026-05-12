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

API_KEY = os.getenv("KOSIS_API_KEY")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
S3_BUCKET = os.getenv("S3_BUCKET")
TOPIC = "kosis-population"

print(f"[debug] KOSIS_API_KEY: {API_KEY[:6]}..." if API_KEY else "[debug] API_KEY 없음")

s3 = boto3.client("s3", region_name="ap-northeast-2")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

STATS = [
    {"orgId": "101", "tblId": "DT_1B040A3", "name": "시도별인구"},
    {"orgId": "101", "tblId": "DT_1B040B3", "name": "시도별가구"},
]

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
        "endPrdDe": "202512",
        "orgId": org_id,
        "tblId": tbl_id,
    }
    res = requests.get(url, params=params, timeout=15)
    res.raise_for_status()
    return res.json()

def save_to_s3(records: list, stat_name: str):
    if not records:
        return
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key = f"raw/kosis/{today}/{stat_name}.json"
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))

def run():
    total = 0
    for stat in STATS:
        try:
            print(f"  [{stat['name']}] 수집 중...")
            data = fetch_kosis(stat["orgId"], stat["tblId"])
            if isinstance(data, list):
                records = []
                for row in data:
                    record = {
                        "stat_name": stat["name"],
                        "ingested_at": datetime.utcnow().isoformat(),
                        **row,
                    }
                    records.append(record)
                    producer.send(TOPIC, value=record)
                producer.flush()
                save_to_s3(records, stat["name"])
                total += len(records)
                print(f"  [{stat['name']}] {len(records)}건 전송+S3저장")
            else:
                print(f"  [{stat['name']}] 응답 오류: {data}")
            time.sleep(1)
        except Exception as e:
            print(f"  [{stat['name']}] 오류: {e}")
    print(f"[kosis_producer] 완료 — 총 {total}건")

if __name__ == "__main__":
    print("[kosis_producer] 시작")
    run()