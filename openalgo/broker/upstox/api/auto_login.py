"""Headless-browser auto-login for Upstox using a stored TOTP secret.

Upstox's REST API (`authenticate_broker` in this package) only covers step 2
of OAuth: exchanging an authorization `code` for an access token. Step 1 -
the actual mobile number / PIN / TOTP entry - only exists on Upstox's own
hosted login page in the browser; there is no documented API for it.

This module automates that browser step with Playwright so the whole login
(including generating the current TOTP from a stored secret) can run
unattended, e.g. from a daily scheduler before market open.

IMPORTANT - this is best-effort scraping of a third-party login UI:
- Selectors below are based on Upstox's login page structure at the time
  this was written. If Upstox changes their login UI, this WILL break and
  the selectors will need to be updated.
- Run with `headless=False` (see `__main__` below) the first time to watch
  the flow and fix any selector mismatches against the live page before
  relying on it unattended.
- This drives your *own* login with your *own* credentials/TOTP secret -
  it is not bypassing any security control, just automating what you'd
  otherwise click through by hand.
- On any failure a screenshot is saved to `log/upstox_autologin_failure.png`
  to make it easy to see exactly which step broke.
"""

import os
from urllib.parse import parse_qs, urlparse

import pyotp

from utils.logging import get_logger

logger = get_logger(__name__)

LOGIN_TIMEOUT_MS = 30_000
FAILURE_SCREENSHOT_PATH = "log/upstox_autologin_failure.png"


def _save_failure_screenshot(page):
    try:
        os.makedirs(os.path.dirname(FAILURE_SCREENSHOT_PATH), exist_ok=True)
        page.screenshot(path=FAILURE_SCREENSHOT_PATH, full_page=True)
        logger.error(f"Saved failure screenshot to {FAILURE_SCREENSHOT_PATH}")
    except Exception:
        logger.exception("Failed to save Upstox auto-login failure screenshot")


def _wait_enabled_and_click(page, button, timeout_ms: int = 10_000):
    """Click a button only once it's actually enabled.

    Upstox's login form keeps submit buttons `disabled` until client-side
    validation accepts the field value; clicking immediately after `.fill()`
    is a no-op race against that, leaving the form stuck on the same screen
    with no error and no exception raised.
    """
    page.wait_for_function("(el) => !el.disabled", arg=button.element_handle(), timeout=timeout_ms)
    button.click()


def _log_visible_inline_errors(page):
    """Best-effort: log any inline validation/error text Upstox is showing.

    Helpful when a click silently fails to advance the form - e.g. a
    rejected OTP/TOTP usually surfaces as red inline text rather than a
    thrown exception, and that text is otherwise invisible to us.
    """
    try:
        texts = page.locator(
            "[class*='error' i], [role='alert'], [class*='invalid' i]"
        ).all_inner_texts()
        texts = [t.strip() for t in texts if t.strip()]
        if texts:
            logger.warning(f"Upstox inline error text detected: {texts}")
    except Exception:
        pass


def get_authorization_code(headless: bool = True):
    """Drive Upstox's hosted login page end-to-end and return the OAuth `code`.

    Returns:
        tuple: (code: str | None, error: str | None)
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None, (
            "playwright is not installed. Run 'uv add playwright' and "
            "'uv run playwright install chromium'."
        )

    broker_api_key = os.getenv("BROKER_API_KEY")
    redirect_url = os.getenv("REDIRECT_URL")
    mobile_number = os.getenv("UPSTOX_MOBILE_NUMBER")
    pin = os.getenv("UPSTOX_PIN")
    totp_secret = os.getenv("UPSTOX_TOTP_SECRET")
    # Upstox's hosted login page only offers "login with mobile number" (plus
    # a QR-code option that doesn't apply here) - there is no separate
    # Client ID field, confirmed against the live page. UPSTOX_CLIENT_ID is
    # not used for login; mobile number is the only identifier accepted.
    login_identifier = mobile_number

    missing = [
        name
        for name, val in (
            ("BROKER_API_KEY", broker_api_key),
            ("REDIRECT_URL", redirect_url),
            ("UPSTOX_MOBILE_NUMBER", mobile_number),
            ("UPSTOX_PIN", pin),
            ("UPSTOX_TOTP_SECRET", totp_secret),
        )
        if not val
    ]
    if missing:
        return None, f"Missing required .env variables for Upstox auto-login: {', '.join(missing)}"

    dialog_url = (
        "https://api.upstox.com/v2/login/authorization/dialog"
        f"?response_type=code&client_id={broker_api_key}&redirect_uri={redirect_url}"
    )

    totp_code = pyotp.TOTP(totp_secret).now()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        try:
            page.goto(dialog_url, timeout=LOGIN_TIMEOUT_MS)

            # Step 1: mobile number entry ("Login with mobile number" panel;
            # the page also shows a "Login with QR code" panel side-by-side
            # which we ignore).
            login_input = page.locator(
                "input[type='tel'], input[id*='mobile' i], input[name*='mobile' i]"
            ).first
            login_input.wait_for(state="visible", timeout=LOGIN_TIMEOUT_MS)
            login_input.fill(login_identifier)
            get_otp_button = page.get_by_role("button", name="Get OTP")
            get_otp_button.wait_for(state="visible", timeout=LOGIN_TIMEOUT_MS)
            _wait_enabled_and_click(page, get_otp_button)

            # Step 2: Upstox defaults to SMS OTP. If 2FA is configured for an
            # authenticator app, there is usually a link to switch to it -
            # try the common phrasing, but don't fail the flow if it's not
            # there (some accounts go straight to the TOTP field).
            switch_to_totp = page.locator("text=/use authenticator|enter totp|use totp/i").first
            try:
                switch_to_totp.wait_for(state="visible", timeout=5_000)
                switch_to_totp.click()
            except Exception:
                logger.debug("No 'switch to authenticator app' link found, continuing")

            # Step 3: TOTP entry. Upstox renders OTP as either one combined
            # field or six single-digit boxes - handle both.
            totp_inputs = page.locator(
                "input[id*='totp' i], input[name*='totp' i], input[autocomplete='one-time-code']"
            )
            totp_inputs.first.wait_for(state="visible", timeout=LOGIN_TIMEOUT_MS)
            count = totp_inputs.count()
            if count > 1:
                for i, digit in enumerate(totp_code):
                    totp_inputs.nth(i).fill(digit)
            else:
                totp_inputs.first.fill(totp_code)

            continue_button = page.get_by_role("button", name="Continue")
            continue_button.wait_for(state="visible", timeout=LOGIN_TIMEOUT_MS)
            try:
                _wait_enabled_and_click(page, continue_button, timeout_ms=5_000)
            except Exception:
                # .fill() sets the value via JS in one shot; some OTP widgets
                # only run their enable-validation off real per-character
                # keyboard events. Re-type it character by character as a
                # human would and retry (only applies to the single
                # combined-field case - the six-box case already fills one
                # character at a time above).
                if count <= 1:
                    logger.debug(
                        "Continue button still disabled after fill(), retyping TOTP via keystrokes"
                    )
                    totp_inputs.first.fill("")
                    totp_inputs.first.press_sequentially(totp_code, delay=80)
                _wait_enabled_and_click(page, continue_button, timeout_ms=5_000)

            # The click above may have silently failed validation (rejected
            # OTP/expired code) and left us on the same screen with no
            # exception raised - surface that instead of timing out blindly
            # on the next step.
            page.wait_for_timeout(1_500)
            _log_visible_inline_errors(page)

            # Step 4: 6-digit PIN entry for returning users.
            pin_input = page.locator(
                "input[type='password'], input[id*='pin' i], input[name*='pin' i]"
            ).first
            pin_input.wait_for(state="visible", timeout=LOGIN_TIMEOUT_MS)
            pin_input.fill(pin)
            login_pin_button = page.get_by_role("button", name="Continue").or_(
                page.get_by_role("button", name="Login")
            )
            _wait_enabled_and_click(page, login_pin_button.first, timeout_ms=5_000)

            # Step 5: wait for the redirect back to our REDIRECT_URL with ?code=...
            page.wait_for_url(f"{redirect_url}*", timeout=LOGIN_TIMEOUT_MS)
            final_url = page.url
            code = parse_qs(urlparse(final_url).query).get("code", [None])[0]

            if not code:
                _save_failure_screenshot(page)
                return None, f"Reached redirect URL but no 'code' param found: {final_url}"

            logger.info("Upstox auto-login: authorization code obtained")
            return code, None

        except Exception as e:
            logger.exception("Upstox auto-login failed during browser automation")
            _log_visible_inline_errors(page)
            _save_failure_screenshot(page)
            return None, f"Upstox auto-login automation error: {e}"
        finally:
            browser.close()


def auto_login(headless: bool = True):
    """Full auto-login: browser automation + token exchange + DB persistence.

    Returns:
        tuple: (success: bool, message: str)
    """
    from threading import Thread

    from broker.upstox.api.auth_api import authenticate_broker
    from database.auth_db import upsert_auth
    from database.master_contract_status_db import init_broker_status
    from database.user_db import find_user_by_username
    from utils.auth_utils import (
        async_master_contract_download,
        load_existing_master_contract,
        should_download_master_contract,
    )

    code, error = get_authorization_code(headless=headless)
    if not code:
        return False, error or "Failed to obtain authorization code from Upstox"

    access_token, error = authenticate_broker(code)
    if not access_token:
        return False, error or "Failed to exchange authorization code for an access token"

    admin_user = find_user_by_username()
    if not admin_user:
        return False, "No admin user found in the database - cannot store the Upstox session"

    inserted_id = upsert_auth(admin_user.username, access_token, "upstox")
    if not inserted_id:
        return False, "Failed to store the Upstox auth token in the database"

    init_broker_status("upstox")
    should_download, reason = should_download_master_contract("upstox")
    if should_download:
        Thread(target=async_master_contract_download, args=("upstox",), daemon=True).start()
    else:
        Thread(target=load_existing_master_contract, args=("upstox",), daemon=True).start()
    logger.info(f"Upstox auto-login successful for user {admin_user.username} ({reason})")

    return True, "Upstox auto-login successful"


if __name__ == "__main__":
    # Manual test entrypoint - run headed the first time to watch the flow
    # and adjust selectors against the live Upstox login page:
    #   uv run python -m broker.upstox.api.auto_login
    import sys

    headed = "--headed" in sys.argv
    ok, msg = auto_login(headless=not headed)
    print(("SUCCESS: " if ok else "FAILED: ") + msg)
