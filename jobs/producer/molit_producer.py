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

API_KEY = os.getenv("MOLIT_API_KEY")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
S3_BUCKET = os.getenv("S3_BUCKET")
TOPIC = "molit-transactions"

print(f"[debug] API_KEY: {API_KEY[:10]}..." if API_KEY else "[debug] API_KEY 없음")

s3 = boto3.client("s3", region_name="ap-northeast-2")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

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

YEAR_MONTHS = [
    f"{year}{month:02d}"
    for year in range(2024, 2026)
    for month in range(1, 13)
]

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

def parse_records(xml_text: str, sigungu_code: str, year_month: str):
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xml_text)
    items = root.findall(".//item")
    records = []
    for item in items:
        record = {
            "sigungu_code": sigungu_code,
            "year_month": year_month,
            "ingested_at": datetime.utcnow().isoformat(),
        }
        for child in item:
            record[child.tag.strip()] = child.text.strip() if child.text else None
        records.append(record)
    return records

def save_to_s3(records: list, sigungu_code: str, year_month: str):
    if not records:
        return
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key = f"raw/molit/{today}/{year_month}_{sigungu_code}.json"
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))

def run():
    total = 0
    for year_month in YEAR_MONTHS:
        print(f"[molit_producer] {year_month} 수집 중...")
        for code in SIGUNGU_CODES:
            try:
                xml = fetch_transactions(code, year_month)
                records = parse_records(xml, code, year_month)
                for r in records:
                    producer.send(TOPIC, value=r)
                producer.flush()
                save_to_s3(records, code, year_month)
                total += len(records)
                if records:
                    print(f"  [{year_month}][{code}] {len(records)}건 전송+S3저장")
                time.sleep(0.3)
            except Exception as e:
                print(f"  [{year_month}][{code}] 오류: {e}")
    print(f"[molit_producer] 전체 완료 — 총 {total}건")

if __name__ == "__main__":
    run()