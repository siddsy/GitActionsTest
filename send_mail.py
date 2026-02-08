import json
import os
import re
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage

from playwright.sync_api import sync_playwright

URL = "https://www.agonline.co.nz/saleyard-results"

WAIT_AFTER_SORT_MS = 1200
WAIT_AFTER_CLICK_MS = 4500
MIN_XHR_EXPECTED = 3

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

TO_EMAIL = "dunderhenlin@gmail.com"


def safe_name(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s)[:120] or "unknown"


def extract_xhr_payload() -> dict:
    headless = True  # Actions must be headless

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()

        captured = []

        def on_response(resp):
            try:
                if resp.request.resource_type not in ("xhr", "fetch"):
                    return

                ct = (resp.headers.get("content-type") or "").lower()
                entry = {"url": resp.url, "status": resp.status, "content_type": ct}

                if "application/json" in ct:
                    try:
                        entry["body"] = resp.json()
                    except Exception:
                        entry["body"] = None
                        entry["json_error"] = True
                else:
                    entry["body"] = None

                captured.append(entry)
            except Exception:
                pass

        page.on("response", on_response)

        page.goto(URL, timeout=60000)

        page.wait_for_selector("iframe", timeout=60000)

        powerbi_frame = None
        for frame in page.frames:
            if "powerbi" in frame.url.lower():
                powerbi_frame = frame
                break
        if not powerbi_frame:
            browser.close()
            raise RuntimeError("Power BI iframe not found")

        powerbi_frame.wait_for_selector(".pivotTableCellNoWrap", timeout=60000)

        # Reset state: sort Date twice
        date_header = powerbi_frame.get_by_role("columnheader", name="Date")
        date_header.click()
        powerbi_frame.wait_for_timeout(WAIT_AFTER_SORT_MS)
        date_header.click()
        powerbi_frame.wait_for_timeout(WAIT_AFTER_SORT_MS)

        print("Power BI visual state reset via Date sort toggle")

        # Click first sale
        captured.clear()

        first_sale = powerbi_frame.locator(".pivotTableCellNoWrap").first
        sale_no = first_sale.inner_text().strip()
        print("Clicking sale:", sale_no)

        first_sale.click()
        powerbi_frame.wait_for_timeout(WAIT_AFTER_CLICK_MS)

        browser.close()

        if len(captured) < MIN_XHR_EXPECTED:
            raise RuntimeError(
                f"FAILED: Only captured {len(captured)} XHR/fetch responses after click "
                f"(expected at least {MIN_XHR_EXPECTED})."
            )

        now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        # This is the in-memory “stored variable” result you wanted:
        payload = {
            "source_url": URL,
            "sale_no": sale_no,
            "captured_count": len(captured),
            "captured": captured,
            "utc_timestamp": now,
        }
        return payload


def send_email(payload: dict):
    email_from = os.environ["EMAIL_FROM"]
    email_pass = os.environ["EMAIL_PASS"]

    sale_no = payload.get("sale_no", "unknown")
    count = payload.get("captured_count", 0)
    stamp = payload.get("utc_timestamp", datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"))

    filename = f"sale_{safe_name(str(sale_no))}_{stamp}.json"
    json_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    msg = EmailMessage()
    msg["Subject"] = f"AgOnline extraction test – {sale_no} ({count} XHR)"
    msg["From"] = email_from
    msg["To"] = TO_EMAIL

    msg.set_content(
        "Extraction ran in GitHub Actions.\n\n"
        f"Sale: {sale_no}\n"
        f"Captured XHR/fetch responses: {count}\n"
        f"Timestamp (UTC): {stamp}\n\n"
        "Attached is the full JSON payload captured in-memory."
    )

    # Attach JSON (no file written)
    msg.add_attachment(
        json_bytes,
        maintype="application",
        subtype="json",
        filename=filename,
    )

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(email_from, email_pass)
        server.send_message(msg)

    print(f"Email sent to {TO_EMAIL} with attachment: {filename}")


def main():
    payload = extract_xhr_payload()
    print(f"Captured responses: {payload['captured_count']}")
    send_email(payload)


if __name__ == "__main__":
    main()
