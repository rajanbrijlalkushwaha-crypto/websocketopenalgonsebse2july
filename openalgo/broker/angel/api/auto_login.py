"""Unattended auto-login for Angel One using a stored TOTP secret.

Angel's REST login API accepts clientcode + PIN + TOTP in one call, so no
browser automation is needed — pyotp generates the current 6-digit code from
the stored secret and we POST it directly.

Required .env variables:
    BROKER_API_KEY          Angel One API key (from developer.angelone.in)
    ANGEL_CLIENT_ID         Your client ID, e.g. R253290
    ANGEL_PIN               Your 4-6 digit trading PIN
    ANGEL_TOTP_SECRET       Base-32 TOTP secret from your authenticator app

Optional:
    ANGEL_AUTO_LOGIN_ENABLED  TRUE to enable (default FALSE)
    ANGEL_AUTO_LOGIN_TIME     HH:MM IST (default 08:30), Mon-Fri only
"""

import os

import pyotp

from utils.logging import get_logger

logger = get_logger(__name__)


def auto_login(headless: bool = True):  # headless param kept for API parity with upstox
    """Full auto-login: generate TOTP → REST call → DB persistence.

    Returns:
        tuple: (success: bool, message: str)
    """
    from broker.angel.api.auth_api import authenticate_broker
    from database.auth_db import upsert_auth
    from database.master_contract_status_db import init_broker_status
    from database.user_db import find_user_by_username
    from utils.auth_utils import (
        async_master_contract_download,
        load_existing_master_contract,
        should_download_master_contract,
    )
    from threading import Thread

    client_id   = os.getenv("ANGEL_CLIENT_ID", "").strip()
    pin         = os.getenv("ANGEL_PIN", "").strip()
    totp_secret = os.getenv("ANGEL_TOTP_SECRET", "").strip()

    missing = [name for name, val in [
        ("ANGEL_CLIENT_ID",   client_id),
        ("ANGEL_PIN",         pin),
        ("ANGEL_TOTP_SECRET", totp_secret),
    ] if not val]

    if missing:
        return False, f"Missing required .env variables for Angel auto-login: {', '.join(missing)}"

    # Also need BROKER_API_KEY — authenticate_broker reads it from env directly
    if not os.getenv("BROKER_API_KEY"):
        return False, "BROKER_API_KEY not set in .env (Angel One API key from developer.angelone.in)"

    try:
        totp_code = pyotp.TOTP(totp_secret).now()
    except Exception as e:
        return False, f"Invalid ANGEL_TOTP_SECRET — pyotp could not parse it: {e}"

    logger.info("Angel auto-login: calling REST API with client_id=%s", client_id)
    auth_token, feed_token, error = authenticate_broker(client_id, pin, totp_code)

    if not auth_token:
        return False, f"Angel REST login failed: {error}"

    admin_user = find_user_by_username()
    if not admin_user:
        return False, "No admin user found in the database — cannot store the Angel session"

    inserted_id = upsert_auth(
        admin_user.username, auth_token, "angel",
        feed_token=feed_token, user_id=client_id,
    )
    if not inserted_id:
        return False, "Failed to store the Angel auth token in the database"

    init_broker_status("angel")

    should_download, reason = should_download_master_contract("angel")
    if should_download:
        Thread(target=async_master_contract_download, args=("angel",), daemon=True).start()
    else:
        Thread(target=load_existing_master_contract, args=("angel",), daemon=True).start()

    logger.info("Angel auto-login successful for user %s (%s)", admin_user.username, reason)
    return True, "Angel auto-login successful"
