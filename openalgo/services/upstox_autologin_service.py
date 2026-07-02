"""Daily scheduler that re-authenticates the Upstox broker session automatically.

Indian broker tokens expire daily at ~3:00 AM IST (see CLAUDE.md). Without
this, the Upstox session has to be re-logged-in by hand every trading day
before the option-chain/websocket pipeline can pull data. When enabled, this
runs `broker.upstox.api.auto_login.auto_login()` once a day at a configurable
IST time, using the stored mobile number / PIN / TOTP secret.

Disabled by default - opt in via .env:
    UPSTOX_AUTO_LOGIN_ENABLED = 'TRUE'
    UPSTOX_AUTO_LOGIN_TIME = '08:15'   # IST, 24h HH:MM
"""

import os
import threading

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from utils.logging import get_logger

logger = get_logger(__name__)

_scheduler: BackgroundScheduler | None = None
_lock = threading.Lock()


def _run_auto_login():
    from broker.upstox.api.auto_login import auto_login

    logger.info("Running scheduled Upstox auto-login")
    try:
        success, message = auto_login(headless=True)
        if success:
            logger.info(f"Scheduled Upstox auto-login succeeded: {message}")
        else:
            logger.error(f"Scheduled Upstox auto-login failed: {message}")
    except Exception:
        logger.exception("Unexpected error during scheduled Upstox auto-login")


def init_upstox_autologin_scheduler():
    """Register the daily Upstox auto-login job if enabled via .env. No-op otherwise."""
    global _scheduler

    enabled = os.getenv("UPSTOX_AUTO_LOGIN_ENABLED", "FALSE").strip().upper() == "TRUE"
    if not enabled:
        logger.debug("Upstox auto-login disabled (UPSTOX_AUTO_LOGIN_ENABLED != TRUE)")
        return

    with _lock:
        if _scheduler is not None:
            return

        run_time = os.getenv("UPSTOX_AUTO_LOGIN_TIME", "08:15").strip()
        try:
            hour, minute = (int(part) for part in run_time.split(":"))
        except ValueError:
            logger.error(
                f"Invalid UPSTOX_AUTO_LOGIN_TIME '{run_time}', expected HH:MM. "
                "Falling back to 08:15 IST."
            )
            hour, minute = 8, 15

        _scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
        _scheduler.add_job(
            _run_auto_login,
            trigger=CronTrigger(hour=hour, minute=minute, day_of_week="mon-fri"),
            id="upstox_auto_login",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=300,
        )
        _scheduler.start()
        logger.info(
            f"Upstox auto-login scheduler started, runs daily at {hour:02d}:{minute:02d} IST"
        )


def trigger_upstox_autologin_now(headless: bool = True):
    """Manually run the Upstox auto-login immediately (for testing)."""
    from broker.upstox.api.auto_login import auto_login

    return auto_login(headless=headless)
