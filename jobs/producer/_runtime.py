"""Producer 공통 런타임 유틸 — 데몬 루프 + graceful shutdown + 로깅."""
from __future__ import annotations

import logging
import os
import signal
import sys
import time
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
            log.info("poll done rows=%s", n)
        except Exception as exc:  # 한 번 실패해도 루프는 유지
            log.exception("poll failed: %s", exc)

        # 짧게 잠재워 인터럽트에 즉시 반응
        for _ in range(poll_interval_seconds):
            if _shutdown:
                break
            time.sleep(1)

    log.info("daemon stopped")
