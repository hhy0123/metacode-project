#!/usr/bin/env bash
#
# propberg AWS 초기 셋업 — S3 버킷 + Glue Database 3개 (Bronze/Silver/Gold) 생성.
# 이미 존재하면 skip. 비용 0 (단순 namespace 생성).
#
# 사전 조건:
#   - aws CLI 설치 + 자격증명 (env AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY 또는 ~/.aws)
#   - propberg/.env 의 AWS_REGION, S3_BUCKET 적용
#
# 사용:
#   bash infra/scripts/aws_initial_setup.sh

set -euo pipefail

# .env가 있으면 환경변수로 로드
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../../.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

REGION="${AWS_REGION:-us-east-1}"
BUCKET="${S3_BUCKET:-propberg-lakehouse-hhy}"

echo "[setup] region=$REGION bucket=$BUCKET"

# 1) S3 버킷
if aws s3api head-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null; then
    echo "[setup] S3 버킷 이미 존재: $BUCKET"
else
    echo "[setup] S3 버킷 생성: $BUCKET"
    if [[ "$REGION" == "us-east-1" ]]; then
        aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
    else
        aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
            --create-bucket-configuration LocationConstraint="$REGION"
    fi
fi

# 2) Glue Database 3개 (Bronze/Silver/Gold)
for db in propberg_bronze propberg_silver propberg_gold; do
    if aws glue get-database --name "$db" --region "$REGION" >/dev/null 2>&1; then
        echo "[setup] Glue DB 이미 존재: $db"
    else
        echo "[setup] Glue DB 생성: $db"
        aws glue create-database \
            --database-input "{\"Name\":\"$db\",\"Description\":\"propberg ${db##propberg_} layer\"}" \
            --region "$REGION"
    fi
done

# 3) Athena query result location 디렉토리 (S3 prefix)
aws s3api put-object --bucket "$BUCKET" --key "athena-results/" --region "$REGION" >/dev/null 2>&1 || true
echo "[setup] athena-results/ prefix 준비"

echo "[setup] 완료"
echo "  다음 단계: bash infra/scripts/run_ddl.sh"
