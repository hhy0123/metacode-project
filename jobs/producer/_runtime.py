"""Producer 공통 런타임 유틸 — 데몬 루프 + graceful shutdown + 로깅 + dedupe 상태 관리."""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Callable

_shutdown = False


def _handle_signal(signum, frame) -> None:
    global _shutdown
    _shutdown = True


def install_signal_handlers() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)


def configure_logging(name: str) -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    return logging.getLogger(name)


def run_daemon(
    name: str,
    poll_interval_seconds: int,
    once: Callable[[], int],
) -> None:
    """`once`를 주기적으로 실행하며 SIGTERM/SIGINT 시 안전 종료."""
    log = configure_logging(name)
    install_signal_handlers()
    log.info("daemon start interval=%ss", poll_interval_seconds)

    while not _shutdown:
        try:
            n = once()
            log.info("poll done new_rows=%s", n)
        except Exception as exc:  # 한 번 실패해도 루프는 유지
            log.exception("poll failed: %s", exc)

        # 짧게 잠재워 인터럽트에 즉시 반응
        for _ in range(poll_interval_seconds):
            if _shutdown:
                break
            time.sleep(1)

    log.info("daemon stopped")


# -------------------------------------------------------------------
# Dedupe — Producer가 매 폴링마다 같은 데이터를 또 publish하지 않도록 한다.
#
# 외부 부동산 API는 webhook을 제공하지 않으므로 client(producer) 측에서
# polling으로 fetch한다. 정직한 streaming은 아니지만, 마지막 본 row 키를
# state 파일에 보관해 "새 거래만 Kafka에 publish" 하도록 만들어 streaming
# 의미를 살린다.
#
# - 컨테이너 재시작 시 복구되도록 파일(JSON)에 저장
# - 메모리 폭증 방지: 최대 MAX_SEEN개 유지 (LRU)
# -------------------------------------------------------------------

_DEFAULT_STATE_DIR = Path(os.getenv("PRODUCER_STATE_DIR", "/tmp/propberg_state"))
_MAX_SEEN = int(os.getenv("PRODUCER_MAX_SEEN_KEYS", "200000"))


class SeenSet:
    """본 적 있는 키를 기억해 새 row만 통과시키는 set + JSON 파일 영속화."""

    def __init__(self, name: str) -> None:
        self._path = _DEFAULT_STATE_DIR / f"{name}_seen.json"
        self._keys: list[str] = []
        self._set: set[str] = set()
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            keys = json.loads(self._path.read_text(encoding="utf-8"))
            if isinstance(keys, list):
                self._keys = list(keys)[-_MAX_SEEN:]
                self._set = set(self._keys)
        except Exception:
            # 손상된 파일은 무시하고 빈 상태로 시작
            self._keys = []
            self._set = set()

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._keys), encoding="utf-8")

    def seen(self, key: str) -> bool:
        return key in self._set

    def add(self, key: str) -> None:
        if key in self._set:
            return
        self._set.add(key)
        self._keys.append(key)
        if len(self._keys) > _MAX_SEEN:
            # LRU 방출
            evict = self._keys[: len(self._keys) - _MAX_SEEN]
            for k in evict:
                self._set.discard(k)
            self._keys = self._keys[len(self._keys) - _MAX_SEEN :]

    def flush(self) -> None:
        self._save()
