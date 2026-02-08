import json
import os
import re
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

from playwright.sync_api import sync_playwright

# ---------------- CONFIG ----------------
URL = "https://www.agonline.co.nz/saleyard-results"

WAIT_AFTER_SORT_MS = 1200
WAIT_AFTER_CLICK_MS = 4500
MIN_XHR_EXPECTED = 3

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
TO_EMAIL = "dunderhenlin@gmail.com"

DEBUG_DIR = Path("debug")
DEBUG_DIR.mkdir(exist_ok=True)


# ---------------- HELPERS ----------------
def safe_name(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s)[:120] or "unknown"


# ---------------- EXTRACTION ----------------
def extract_xhr_payload() -> dict:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )

        context = browser.new_context(
            viewport={"width": 1366, "height": 900},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-NZ",
        )

        page = context.new_page()
        captured = []

        def on_response(resp):
            try:
                if resp.request.resource_type not in ("xhr", "fetch"):
                    return

                ct = (resp.headers.get("content-type") or "").lower()
                entry = {
                    "url": resp.url,
                    "status": resp.status,
                    "content_type": ct,
                }

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

        try:
            page.goto(URL, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(4000)

            page.wait_for_selector("iframe", state="attached", timeout=90000)

            powerbi_frame = None
            for _ in range(90):
                for frame in page.frames:
                    if "powerbi" in frame.url.lower():
                        powerbi_frame = frame
                        break
                if powerbi_frame:
                    break
                page.wait_for_timeout(1000)

            if not powerbi_frame:
                page.screenshot(path=str(DEBUG_DIR / "no_powerbi_frame.png"), full_page=True)
                (DEBUG_DIR / "no_powerbi_frame.html").write_text(page.content(), encoding="utf-8")
                raise RuntimeError("Power BI iframe never appeared")

            powerbi_frame.wait_for_selector(".pivotTableCellNoWrap", timeout=90000)

            date_header = powerbi_frame.get_by_role("columnheader", name="Date")
            date_header.click()
            powerbi_frame.wait_for_timeout(WAIT_AFTER_SORT_MS)
            date_header.click()
            powerbi_frame.wait_for_timeout(WAIT_AFTER_SORT_MS)

            captured.clear()

            first_sale = powerbi_frame.locator(".pivotTableCellNoWrap").first
            sale_no = first_sale.inner_text().strip()
            print("Clicking sale:", sale_no)

            first_sale.click()
            powerbi_frame.wait_for_timeout(WAIT_AFTER_CLICK_MS)

            if len(captured) < MIN_XHR_EXPECTED:
                page.screenshot(path=str(DEBUG_DIR / "clicked_no_xhr.png"), full_page=True)
                (DEBUG_DIR / "clicked_no_xhr.html").write_text(page.content(), encoding="utf-8")
                raise RuntimeError(
                    f"Only {len(captured)} XHR/fetch responses captured"
                )

            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

            return {
                "source_url": URL,
                "sale_no": sale_no,
                "captured_count": len(captured),
                "captured": captured,
                "utc_timestamp": stamp,
            }

        except Exception:
            try:
                page.screenshot(path=str(DEBUG_DIR / "failure.png"), full_page=True)
                (DEBUG_DIR / "failure.html").write_text(page.content(), encoding="utf-8")
            except Exception:
                pass
            raise

        finally:
            browser.close()


# ---------------- EMAIL ----------------
def send_email(payload: dict):
    email_from = os.environ["EMAIL_FROM"]
    email_pass = os.environ["EMAIL_PASS"]

    sale_no = payload.get("sale_no", "unknown")
    stamp = payload.get("utc_timestamp")
    count = payload.get("captured_count", 0)

    filename = f"sale_{safe_name(str(sale_no))}_{stamp}.json"
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    msg = EmailMessage()
    msg["Subject"] = f"AgOnline extraction test – {sale_no} ({count} XHR)"
    msg["From"] = email_from
    msg["To"] = TO_EMAIL
    msg.set_content(
        "GitHub Actions extraction run.\n\n"
        f"Sale: {sale_no}\n"
        f"Captured XHR: {count}\n"
        f"UTC: {stamp}\n\n"
        "JSON payload attached."
    )

    msg.add_attachment(
        data,
        maintype="application",
        subtype="json",
        filename=filename,
    )

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.starttls()
        server.login(email_from, email_pass)
        server.send_message(msg)

    print("Email sent successfully")


# ---------------- MAIN ----------------
def main():
    payload = extract_xhr_payload()
    print("Captured:", payload["captured_count"])
    send_email(payload)


if __name__ == "__main__":
    main()
