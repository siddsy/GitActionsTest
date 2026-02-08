import json
import os
import re
import glob
import smtplib
from datetime import datetime
from pathlib import Path
from email.message import EmailMessage

from playwright.sync_api import sync_playwright

# -----------------------
# CONFIG
# -----------------------
URL = "https://www.agonline.co.nz/saleyard-results"

OUT_DIR = Path("xhr_dump")
OUT_DIR.mkdir(exist_ok=True)

WAIT_AFTER_SORT_MS = 1200
WAIT_AFTER_CLICK_MS = 4500
MIN_XHR_EXPECTED = 3

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

TO_EMAIL = "dunderhenlin@gmail.com"


def safe_name(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s)[:120]


def newest_json() -> Path:
    files = glob.glob("xhr_dump/*.json")
    if not files:
        raise RuntimeError("No JSON files found in xhr_dump/")
    files.sort(key=os.path.getmtime, reverse=True)
    return Path(files[0])


def extract_xhr_and_save() -> Path:
    headless = True
    # locally you might want headful, but in Actions it must be headless
    if os.getenv("CI", "").lower() in ("", "0", "false", "no"):
        # if you want local headful testing, set CI=0
        headless = False

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

                entry = {
                    "url": resp.url,
                    "status": resp.status,
                    "content_type": ct,
                }

                if "application/json" in ct:
                    entry["body"] = resp.json()
                else:
                    entry["body"] = None

                captured.append(entry)
            except Exception:
                # don't kill the run on a single bad response parse
                pass

        page.on("response", on_response)

        # Load page (no networkidle)
        page.goto(URL, timeout=60000)

        # Find Power BI iframe
        page.wait_for_selector("iframe", timeout=60000)

        powerbi_frame = None
        for frame in page.frames:
            if "powerbi" in frame.url.lower():
                powerbi_frame = frame
                break

        if not powerbi_frame:
            browser.close()
            raise RuntimeError("Power BI iframe not found")

        # Wait for table cells inside iframe
        powerbi_frame.wait_for_selector(".pivotTableCellNoWrap", timeout=60000)

        # Reset visual state: sort Date twice
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

        if len(captured) < MIN_XHR_EXPECTED:
            browser.close()
            raise RuntimeError(
                f"FAILED: Only captured {len(captured)} XHR/fetch responses after click "
                f"(expected at least {MIN_XHR_EXPECTED})."
            )

        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        out_file = OUT_DIR / f"sale_{safe_name(sale_no)}_{stamp}.json"

        out_file.write_text(
            json.dumps(
                {
                    "sale_no": sale_no,
                    "captured_count": len(captured),
                    "captured": captured,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        print(f"SUCCESS: Captured {len(captured)} XHR responses")
        print(f"Saved → {out_file}")

        browser.close()
        return out_file


def send_email_with_attachment(attachment: Path):
    email_from = os.environ["EMAIL_FROM"]
    email_pass = os.environ["EMAIL_PASS"]

    msg = EmailMessage()
    msg["Subject"] = f"AgOnline extraction test – {attachment.name}"
    msg["From"] = email_from
    msg["To"] = TO_EMAIL
    msg.set_content(
        "Extraction test from GitHub Actions.\n\n"
        f"Attached: {attachment.name}\n"
        "If you received this, extraction + email both worked."
    )

    msg.add_attachment(
        attachment.read_bytes(),
        maintype="application",
        subtype="json",
        filename=attachment.name,
    )

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(email_from, email_pass)
        server.send_message(msg)

    print(f"Email sent to {TO_EMAIL} with attachment: {attachment.name}")


def main():
    # 1) Run extraction (this is what you want to test)
    out_file = extract_xhr_and_save()

    # 2) Email the output JSON
    send_email_with_attachment(out_file)


if __name__ == "__main__":
    main()
