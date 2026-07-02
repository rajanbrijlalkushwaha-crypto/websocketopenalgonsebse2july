"""Daily scheduler that re-authenticates the Angel One broker session automatically.

Angel One tokens expire daily at ~3:00 AM IST. Without this, the session has
to be re-logged-in by hand every trading day. When enabled, this runs
`broker.angel.api.auto_login.auto_login()` once a day at a configurable IST
time, using the stored client ID / PIN / TOTP secret — no browser needed.

Opt in via .env:
    ANGEL_AUTO_LOGIN_ENABLED = TRUE
    ANGEL_AUTO_LOGIN_TIME    = 08:30   # IST, 24h HH:MM, Mon-Fri only
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
    from broker.angel.api.auto_login import auto_login

    logger.info("Running scheduled Angel auto-login")
    try:
        success, message = auto_login()
        if success:
            logger.info("Scheduled Angel auto-login succeeded: %s", message)
        else:
            logger.error("Scheduled Angel auto-login failed: %s", message)
    except Exception:
        logger.exception("Unexpected error during scheduled Angel auto-login")


def init_angel_autologin_scheduler():
    """Register the daily Angel auto-login job if enabled via .env. No-op otherwise."""
    global _scheduler

    enabled = os.getenv("ANGEL_AUTO_LOGIN_ENABLED", "FALSE").strip().upper() == "TRUE"
    if not enabled:
        logger.debug("Angel auto-login disabled (ANGEL_AUTO_LOGIN_ENABLED != TRUE)")
        return

    with _lock:
        if _scheduler is not None:
            return

        run_time = os.getenv("ANGEL_AUTO_LOGIN_TIME", "08:30").strip()
        try:
            hour, minute = (int(p) for p in run_time.split(":"))
        except ValueError:
            logger.error(
                "Invalid ANGEL_AUTO_LOGIN_TIME '%s', expected HH:MM. Falling back to 08:30 IST.",
                run_time,
            )
            hour, minute = 8, 30

        _scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
        _scheduler.add_job(
            _run_auto_login,
            trigger=CronTrigger(hour=hour, minute=minute, day_of_week="mon-fri"),
            id="angel_auto_login",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=300,
        )
        _scheduler.start()
        logger.info(
            "Angel auto-login scheduler started, runs daily at %02d:%02d IST", hour, minute
        )


def trigger_angel_autologin_now():
    """Manually trigger the Angel auto-login immediately (for testing)."""
    from broker.angel.api.auto_login import auto_login

    return auto_login()
