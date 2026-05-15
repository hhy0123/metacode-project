# Superset 대시보드

propberg는 **운영 가시성 4-Tier 중 T3 (비즈니스)** 를 Apache Superset으로 담당한다 (`docs/observability.md` 참고).
Athena → Superset 직접 연결 → Gold 테이블 기반 시각화.

---

## 현재 보관 형태 — 솔직 명시

Superset 메타데이터(데이터소스, 차트, 대시보드 정의)는 **`propberg-superset` 컨테이너 내부의 SQLite** 에 저장된다. 컨테이너 named volume 마운트는 별도로 하지 않아 컨테이너 삭제 시 사라진다.

본 디렉토리에는 **IaC export 파일이 아직 없다**. Phase 2 작업으로 다음을 자동화할 계획:
- `superset export-dashboards`로 JSON/ZIP export 후 git 커밋
- 컨테이너 기동 시 `superset import-dashboards`로 자동 import
- 데이터소스 (Athena connection)는 환경변수 + provisioning script로 관리

현재 (Phase 1)는 수동 셋업 + 발표 시연용 SQLite 보관에 머무른다.

---

## 발표 시연용 대시보드 (수동 생성)

발표 직전 Superset UI에서 만든 차트 3개 + 대시보드 1개. Athena 연결 후 `propberg_gold` 데이터셋 3개 등록 → 차트 생성.

| # | 차트 이름 | 타입 | 데이터셋 | METRIC / DIMENSION |
|---|---|---|---|---|
| 1 | 시도별 거래 | Bar Chart | `propberg_gold.daily_trade_summary` | `SUM(trade_count)` × `sido` |
| 2 | 급등/급락 비율 | Pie Chart | `propberg_gold.anomaly_transactions` | `COUNT(*)` × `anomaly_type` |
| 3 | 총 거래 건수 | Big Number | `propberg_gold.daily_trade_summary` | `SUM(trade_count)` |

대시보드: **propberg 부동산 분석 대시보드** — 위 3개 차트를 묶음.

> 발표 캡처: 경기도가 거래 1위(약 2,000건), 서울 2위, 부산/인천/대구 순. 실제 한국 부동산 시장 거래 비중과 일치.

---

## Athena 연결 setup (수동)

발표 환경에서는 다음 절차로 매번 셋업한다. Phase 2에서 자동화 대상.

### 1) Superset에 pyathena 드라이버 설치 (이미지에 없음)

```bash
docker exec -u root propberg-superset pip install "PyAthena[SQLAlchemy]"
docker compose --env-file .env -f infra/docker/docker-compose.yml restart superset
```

> 컨테이너 recreate 시 (`up --force-recreate`) pyathena가 사라진다 — Phase 2에서 `Dockerfile.superset`로 영구화 계획.

### 2) Database 연결

Superset UI → Settings → Database Connections → + DATABASE → Amazon Athena (또는 Other) → SQLAlchemy URI:

```
awsathena+rest://{ACCESS_KEY_ID}:{URL_ENCODED_SECRET}@athena.{REGION}.amazonaws.com:443/propberg_gold?s3_staging_dir=s3%3A%2F%2F{S3_BUCKET}%2Fathena-results%2F
```

> `{URL_ENCODED_SECRET}`: AWS Secret Key에 `/`, `+`, `=` 있으면 URL 인코딩 필요 (`/` → `%2F`).
> 자동 생성: `[uri]::EscapeDataString("$SECRET")` (PowerShell) 또는 `python -c "import urllib.parse;print(urllib.parse.quote('$SECRET',safe=''))"`.

### 3) 데이터셋 등록

Data → Datasets → + DATASET → Schema `propberg_gold` → 다음 3개:
- `daily_trade_summary`
- `monthly_price_trend`
- `anomaly_transactions`

(필요 시 `streaming_health`도 추가 — 운영 가시성용)

---

## Phase 2 — IaC 자동화 로드맵

1. **Dockerfile.superset 작성** — `apache/superset:2.1.3` base + `pip install PyAthena[SQLAlchemy]`
2. **`databases/athena.yaml`** — Superset CLI import용 datasource 정의 (환경변수로 키 주입)
3. **`datasets/*.yaml`** — Gold 4개 테이블 데이터셋 정의
4. **`dashboards/main.zip`** — `superset export-dashboards`로 받은 ZIP을 git에 커밋
5. **컨테이너 entrypoint**에서 `superset import-dashboards` 자동 실행

5단계 모두 완료되면 `docker compose up`만으로 대시보드까지 자동 복구 가능.
