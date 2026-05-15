#!/usr/bin/env bash
#
# propberg DDL 일괄 적용 — ddl/bronze.sql, silver.sql, gold.sql 의 모든 CREATE TABLE을
# 순서대로 Athena에 실행한다.
#
# Athena는 한 번에 한 SQL만 받으므로 ';'로 split해서 각각 start_query_execution 호출.
# 각 query SUCCEEDED 까지 polling.
#
# 사전 조건:
#   - aws CLI + boto3 (python3 -m pip install boto3)
#   - aws_initial_setup.sh 가 먼저 실행되어 Glue DB가 존재해야 함
#   - propberg/.env 의 AWS_REGION, S3_BUCKET, ATHENA_WORKGROUP 적용
#
# 사용:
#   bash infra/scripts/run_ddl.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../../.env"
DDL_DIR="${SCRIPT_DIR}/../../ddl"

if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

export REGION="${AWS_REGION:-us-east-1}"
export BUCKET="${S3_BUCKET:-propberg-lakehouse-hhy}"
export WORKGROUP="${ATHENA_WORKGROUP:-primary}"
export OUTPUT_LOC="s3://${BUCKET}/athena-results/"

echo "[ddl] region=$REGION workgroup=$WORKGROUP output=$OUTPUT_LOC"

run_file() {
    local sql_file="$1"
    echo "[ddl] === $sql_file ==="
    SQL_FILE="$sql_file" python3 <<'PYEOF'
import os, re, sys, time, boto3

sql_file = os.environ["SQL_FILE"]
region   = os.environ["REGION"]
wg       = os.environ["WORKGROUP"]
output   = os.environ["OUTPUT_LOC"]

with open(sql_file, encoding="utf-8") as fh:
    raw = fh.read()

# 주석 (-- ...) 한 줄 제거 후 ';' 로 split
clean = re.sub(r"--[^\n]*", "", raw)
stmts = [s.strip() for s in clean.split(";") if s.strip()]

if not stmts:
    print(f"[ddl] {sql_file}: 빈 SQL")
    sys.exit(0)

client = boto3.client("athena", region_name=region)
for i, s in enumerate(stmts, 1):
    head = s.split("\n", 1)[0][:80]
    print(f"  [{i}/{len(stmts)}] {head}")
    r = client.start_query_execution(
        QueryString=s,
        WorkGroup=wg,
        ResultConfiguration={"OutputLocation": output},
    )
    qid = r["QueryExecutionId"]
    while True:
        st = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = st["State"]
        if state == "SUCCEEDED":
            print(f"       SUCCEEDED ({qid})")
            break
        if state in ("FAILED", "CANCELLED"):
            reason = st.get("StateChangeReason", "no reason")
            print(f"       {state}: {reason}", file=sys.stderr)
            sys.exit(1)
        time.sleep(2)
PYEOF
}

run_file "${DDL_DIR}/bronze.sql"
run_file "${DDL_DIR}/silver.sql"
run_file "${DDL_DIR}/gold.sql"

echo "[ddl] 모든 DDL 적용 완료"
