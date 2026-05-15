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
from _runtime import run_daemon, SeenSet

_seen = SeenSet("molit")

env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)

API_KEY = os.getenv("MOLIT_API_KEY")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
S3_BUCKET = os.getenv("S3_BUCKET")
TOPIC = "molit-transactions"
POLL_INTERVAL = int(os.getenv("MOLIT_POLL_INTERVAL", "300"))  # 기본 5분

SIGUNGU_CODES = [
    "11110", "11140", "11170", "11200", "11215",
    "11230", "11260", "11290", "11305", "11320",
    "11350", "11380", "11410", "11440", "11470",
    "11500", "11530", "11545", "11560", "11590",
    "11620", "11650", "11680", "11710", "11740",
    "41111", "41113", "41115", "41117", "41119",
    "41121", "41131", "41133", "41135", "41150",
    "41171", "41173", "41195", "41197", "41199",
    "41210", "41220", "41250", "41270", "41281",
    "41285", "41290", "41310", "41360", "41370",
    "41390", "41410", "41430", "41450", "41461",
    "41463", "41465", "41480", "41500", "41550",
    "41570", "41590", "41610", "41630", "41650",
    "41670", "41690", "41710", "41720", "41730",
    "28110", "28140", "28170", "28185", "28200",
    "28237", "28245", "28260", "28710", "28720",
    "26110", "26140", "26170", "26200", "26230",
    "26260", "26290", "26320", "26350", "26380",
    "26410", "26440", "26470", "26500", "26530",
    "26710",
    "27110", "27140", "27170", "27200", "27230",
    "27260", "27290", "27710",
    "29110", "29140", "29155", "29170", "29200",
    "30110", "30140", "30170", "30200", "30230",
    "31110", "31140", "31170", "31200", "31710",
    "36110",
]

s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

def fetch_transactions(sigungu_code: str, year_month: str):
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    params = {
        "serviceKey": API_KEY,
        "LAWD_CD": sigungu_code,
        "DEAL_YMD": year_month,
        "numOfRows": 100,
        "pageNo": 1,
    }
    res = requests.get(url, params=params, timeout=10)
    res.raise_for_status()
    return res.text

def _deal_key(record: dict, sigungu_code: str) -> str:
    """거래 고유 키 — 같은 거래는 한 번만 publish하기 위한 dedupe key."""
    return "|".join(
        [
            sigungu_code,
            record.get("aptSeq", "") or "",
            record.get("dealYear", "") or "",
            record.get("dealMonth", "") or "",
            record.get("dealDay", "") or "",
            record.get("dealAmount", "") or "",
            record.get("floor", "") or "",
        ]
    )


def parse_and_send(xml_text: str, sigungu_code: str, year_month: str):
    """국토부 API는 월 단위 전체 결과를 돌려줘 매 폴링마다 같은 row가 다시 들어온다.
    여기서 producer 측 dedupe — 이미 본 거래는 skip, 새 거래만 Kafka에 publish."""
    import xml.etree.ElementTree as ET

    root = ET.fromstring(xml_text)
    items = root.findall(".//item")
    new_records = []
    for item in items:
        record = {
            "sigungu_code": sigungu_code,
            "year_month": year_month,
            "ingested_at": datetime.utcnow().isoformat(),
        }
        for child in item:
            record[child.tag.strip()] = child.text.strip() if child.text else None

        key = _deal_key(record, sigungu_code)
        if _seen.seen(key):
            continue  # 이전 폴링에서 이미 본 거래 → skip
        _seen.add(key)

        producer.send(TOPIC, value=record)
        new_records.append(record)

    producer.flush()

    if new_records:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        key = f"raw/molit/{today}/{year_month}_{sigungu_code}.json"
        body = "\n".join(json.dumps(r, ensure_ascii=False) for r in new_records)
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))

    return len(new_records)

def run_once():
    year_month = datetime.now().strftime("%Y%m")
    print(f"[molit_producer] {year_month} 폴링 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')} KST)")
    new_total = 0
    for code in SIGUNGU_CODES:
        try:
            xml = fetch_transactions(code, year_month)
            count = parse_and_send(xml, code, year_month)
            new_total += count
            time.sleep(0.3)
        except Exception as e:
            print(f"  [{code}] 오류: {e}")
    _seen.flush()  # dedupe 상태 영속화 — 컨테이너 재시작 시에도 중복 publish 방지
    print(f"[molit_producer] 폴링 완료 — 새 거래 {new_total}건 → Kafka Topic: {TOPIC}")
    return new_total

if __name__ == "__main__":
    run_daemon("molit_producer", POLL_INTERVAL, run_once)