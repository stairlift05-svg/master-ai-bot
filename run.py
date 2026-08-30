#!/usr/bin/env python3
"""Live engine entry point.

Startup order:

1. load ``.env`` and build validated :class:`Settings`,
2. configure structured logging (secret redaction active),
3. start the Flask dashboard thread (0.0.0.0:PORT),
4. start the asyncio engine (health check -> warmup -> four loops + Telegram),
5. handle SIGINT/SIGTERM with a clean shutdown.
"""
from __future__ import annotations

import asyncio
import signal
import sys
import threading
import time  # noqa: F401
import traceback
from typing import Optional

sys.path.insert(0, ".")

from app.config import Settings  # noqa: E402
from app.core.engine import QuantEngine  # noqa: E402
from app.errors import ConfigError  # noqa: E402
from app.observability.logging_setup import setup_logging  # noqa: E402
from app.security.secrets import load_secrets  # noqa: E402
from app.server.web import create_app  # noqa: E402
from app.state import EngineState  # noqa: E402

import logging

log = logging.getLogger("quant.main")


def run_web(settings: Settings, state: EngineState, db) -> None:
    """Run the Flask dashboard in a daemon thread."""
    app = create_app(state, db, settings)
    log.info("Flask dashboard on 0.0.0.0:%d", settings.port)
    app.run(host="0.0.0.0", port=settings.port, debug=False,
            use_reloader=False, threaded=True)


async def _amain(settings: Settings, state: EngineState) -> None:
    engine = QuantEngine(settings, state)

    web_thread = threading.Thread(
        target=run_web, args=(settings, state, engine.db), daemon=True,
        name="flask-dashboard",
    )
    web_thread.start()

    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()

    def _cancel(*_args) -> None:
        if main_task is not None and not main_task.done():
            main_task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _cancel)
        except (NotImplementedError, RuntimeError):  # pragma: no cover
            pass

    await engine.start()
    log.info("Engine running. Press Ctrl+C to stop.")
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        log.info("Shutdown signal received — closing engine…")
        await engine.shutdown()
        raise


def main() -> int:
    load_secrets()
    settings = Settings.from_env()
    try:
        settings.validate()
    except ConfigError as exc:
        print(f"❌ Configuration error:\n{exc}", file=sys.stderr)
        print("Fix .env (see .env.example) and restart.", file=sys.stderr)
        return 2

    setup_logging(settings.log_level, settings.log_file or None)
    log.info("=== IMBA ALGO Engine v23 (AriaX) starting ===")
    log.info("base=%s symbols=%s", settings.arlax_base, list(settings.symbols))

    state = EngineState()
    try:
        asyncio.run(_amain(settings, state))
    except asyncio.CancelledError:
        pass  # clean shutdown path already ran
    except KeyboardInterrupt:
        log.info("Interrupted")
    except Exception as exc:  # noqa: BLE001 - top-level crash guard
        log.exception("FATAL: %s", exc)
        traceback.print_exc()
        return 1
    log.info("Bye.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
