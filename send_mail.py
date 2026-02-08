# ==========================================
# AgOnline PowerBI → XLSX (3 sheets) + EMAIL (GitHub Actions safe)
# - 3 clicks: Date header twice + first sale click
# - Captures PowerBI JSON payloads
# - Reconstructs rows
# - Writes XLSX with sheets: sales, summary_rows, breakdown_rows
# - Emails the XLSX (uses EMAIL_FROM / EMAIL_PASS secrets)
# ==========================================

import asyncio
import json
import os
import re
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

# ---------------- CONFIG ----------------
URL = "https://www.agonline.co.nz/saleyard-results"

WAIT_AFTER_SORT_MS = 1200
WAIT_AFTER_CLICK_MS = 6500
MIN_XHR_EXPECTED = 3

# In Actions: headless must be True
HEADLESS = os.getenv("CI", "").lower() in ("1", "true", "yes")

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
TO_EMAIL = "dunderhenlin@gmail.com"

OUT_DIR = Path("output")
OUT_DIR.mkdir(exist_ok=True)

DEBUG_DIR = Path("debug")
DEBUG_DIR.mkdir(exist_ok=True)

MEASURES = [
    "qty_offered", "qty_sold",
    "avg_price_kg", "min_price_kg", "max_price_kg",
    "avg_price_hd", "min_price_hd", "max_price_hd"
]
KG_COLS = ["avg_price_kg", "min_price_kg", "max_price_kg"]
HD_COLS = ["avg_price_hd", "min_price_hd", "max_price_hd"]

KEYS = [
    "sale_no", "sale_key", "capture_id", "sale_date", "saleyard", "raw_title",
    "table_type", "row_level", "section", "class",
    "weight_range", "weight_from", "weight_to"
]

# ---------------- HELPERS ----------------
def safe_get(obj, path, default=None):
    cur = obj
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        elif isinstance(cur, list) and isinstance(p, int) and 0 <= p < len(cur):
            cur = cur[p]
        else:
            return default
    return cur


def coerce_num(x):
    if isinstance(x, (int, float)):
        return x
    if isinstance(x, str):
        try:
            return float(x)
        except Exception:
            return x
    return x


def parse_weight_range(w):
    if isinstance(w, str) and "-" in w:
        try:
            a, b = w.split("-", 1)
            return int(a.strip()), int(b.strip())
        except Exception:
            pass
    return None, None


def get_value_dicts(dsr):
    try:
        return dsr["DS"][0].get("ValueDicts", {}) or {}
    except Exception:
        return {}


def decode_vdict(schema_entry, raw_value, value_dicts):
    dn = schema_entry.get("DN")
    if dn and dn in value_dicts and isinstance(raw_value, int):
        vd = value_dicts[dn]
        if 0 <= raw_value < len(vd):
            return vd[raw_value]
    return raw_value


def norm_measure(name: str):
    n = (name or "").lower()

    if "quantity offered" in n:
        return "qty_offered"
    if "quantity sold" in n:
        return "qty_sold"

    if "price/kg" in n:
        if "ave" in n or "avg" in n:
            return "avg_price_kg"
        if "min" in n:
            return "min_price_kg"
        if "max" in n:
            return "max_price_kg"

    if "price/hd" in n:
        if "ave" in n or "avg" in n:
            return "avg_price_hd"
        if "min" in n:
            return "min_price_hd"
        if "max" in n:
            return "max_price_hd"

    return None


def build_maps(descriptor):
    select = descriptor.get("Select", []) or []
    dim_map = {}
    meas_map = {}

    for s in select:
        if not isinstance(s, dict):
            continue

        kind = s.get("Kind")
        val = s.get("Value")
        nm = (s.get("Name", "") or "")
        lnm = nm.lower()

        # dimensions
        if kind == 1 and isinstance(val, str) and val.startswith("G"):
            if "section" in lnm and "description" in lnm:
                dim_map[val] = "section"
            elif "weight" in lnm:
                dim_map[val] = "weight_range"
            elif "class" in lnm:
                dim_map[val] = "class"
            else:
                dim_map[val] = f"dim_{val}"

        # measures (+ subtotal aliases)
        if kind == 2 and isinstance(val, str) and val.startswith("M"):
            m = norm_measure(nm)
            if m:
                meas_map[val] = m
                for a in (s.get("Subtotal") or []):
                    if isinstance(a, str) and a.startswith("A"):
                        meas_map[a] = m

    # subtotal members
    subtotal_members = set()
    groupings = safe_get(descriptor, ["Expressions", "Primary", "Groupings"], []) or []
    for g in groupings:
        sm = g.get("SubtotalMember")
        if sm:
            subtotal_members.add(sm)

    return dim_map, meas_map, subtotal_members


def bit_is_set(bitset: int, idx: int) -> bool:
    return ((bitset >> idx) & 1) == 1


def reconstruct_full(schema, rec, prev_full):
    C = rec.get("C")
    if not isinstance(C, list):
        return None

    schema_len = len(schema)
    copy_bits = int(rec.get("R", 0) or 0)
    null_bits = int(rec.get("Ø", 0) or 0)

    if copy_bits and (prev_full is None or len(prev_full) != schema_len):
        return None

    full = C[:]
    for i in range(schema_len):
        if bit_is_set(null_bits, i):
            full.insert(i, None)
        if bit_is_set(copy_bits, i):
            full.insert(i, prev_full[i])

    if len(full) != schema_len:
        return None
    return full


def iter_decoded_records(records, stream_state, debug_list, sale_no, payload_idx, dm_key, node_schema=None):
    schema = stream_state.get("schema")
    prev_full = stream_state.get("prev_full")

    if isinstance(node_schema, list) and node_schema:
        schema = node_schema
        stream_state["schema"] = schema

    for rec_idx, rec in enumerate(records):
        if not isinstance(rec, dict):
            continue

        if isinstance(rec.get("S"), list) and rec["S"]:
            schema = rec["S"]
            stream_state["schema"] = schema
            prev_full = None
            stream_state["prev_full"] = None
            continue

        if not schema:
            debug_list.append({"sale_no": sale_no, "payload_idx": payload_idx, "dm": dm_key, "rec_idx": rec_idx, "reason": "no_schema"})
            continue

        full = reconstruct_full(schema, rec, prev_full)
        if full is None:
            debug_list.append({"sale_no": sale_no, "payload_idx": payload_idx, "dm": dm_key, "rec_idx": rec_idx, "reason": "align_fail"})
            continue

        prev_full = full
        stream_state["prev_full"] = prev_full
        yield schema, full


def extract_rows_from_payload(payload, sale_meta, payload_idx, debug_list):
    data = safe_get(payload, ["results", 0, "result", "data"])
    if not isinstance(data, dict):
        return []

    descriptor = data.get("descriptor", {})
    dsr = data.get("dsr", {})
    if not descriptor or not dsr:
        return []

    dim_map, meas_map, subtotal_members = build_maps(descriptor)
    value_dicts = get_value_dicts(dsr)

    ph_list = safe_get(dsr, ["DS", 0, "PH"], []) or []
    if not isinstance(ph_list, list):
        return []

    out_rows = []
    sale_no = sale_meta.get("sale_no")
    stream_states = {}

    for ph in ph_list:
        if not isinstance(ph, dict):
            continue

        for dm_name, nodes in ph.items():
            if not isinstance(nodes, list):
                continue

            for node in nodes:
                if not isinstance(node, dict):
                    continue

                node_schema = node.get("S") if isinstance(node.get("S"), list) else None

                ctx = {}
                for k, v in node.items():
                    if isinstance(k, str) and k.startswith("G") and k in dim_map:
                        ctx[dim_map[k]] = v

                for mblock in (node.get("M", []) or []):
                    if not isinstance(mblock, dict):
                        continue

                    for child_dm, records in mblock.items():
                        if not isinstance(records, list):
                            continue

                        dm_key = f"{dm_name}->{child_dm}"
                        if dm_key not in stream_states:
                            stream_states[dm_key] = {}

                        is_subtotal = child_dm in subtotal_members

                        for schema, full in iter_decoded_records(
                            records, stream_states[dm_key], debug_list, sale_no, payload_idx, dm_key, node_schema
                        ):
                            dims = dict(ctx)
                            measures = {}

                            for idx, sch in enumerate(schema):
                                col = sch.get("N")
                                val = full[idx]

                                if isinstance(col, str) and col.startswith("G"):
                                    std_dim = dim_map.get(col, col)
                                    dims[std_dim] = decode_vdict(sch, val, value_dicts)

                                if isinstance(col, str) and (col.startswith("M") or col.startswith("A")):
                                    mname = meas_map.get(col)
                                    if mname and val is not None:
                                        measures[mname] = coerce_num(val)

                            if is_subtotal:
                                if dims.get("weight_range"):
                                    row_level = "weight_total"
                                elif dims.get("class") and not dims.get("weight_range"):
                                    row_level = "class_total"
                                elif dims.get("section") and not dims.get("class"):
                                    row_level = "section_total"
                                else:
                                    row_level = "overall_total"
                            else:
                                row_level = "detail"

                            wf, wt = parse_weight_range(dims.get("weight_range"))
                            row_table_type = "breakdown" if dims.get("weight_range") else "summary"

                            out_rows.append({
                                **sale_meta,
                                "table_type": row_table_type,
                                "row_level": row_level,
                                "section": dims.get("section"),
                                "class": dims.get("class"),
                                "weight_range": dims.get("weight_range"),
                                "weight_from": wf,
                                "weight_to": wt,
                                **measures
                            })

    return out_rows


# -------- TITLE PICKER (avoids placeholder) --------
def collect_candidate_titles(payloads):
    out = []
    for p in payloads:
        data = safe_get(p, ["results", 0, "result", "data"])
        if not isinstance(data, dict):
            continue

        dsr = data.get("dsr") or {}
        ph0 = safe_get(dsr, ["DS", 0, "PH", 0])
        if not isinstance(ph0, dict):
            continue

        dm0 = ph0.get("DM0")
        if not isinstance(dm0, list) or not dm0:
            continue

        node = dm0[0]
        if not isinstance(node, dict):
            continue

        if isinstance(node.get("M0"), str):
            s = node["M0"].strip()
            if s:
                out.append(s)

        if isinstance(node.get("S"), list) and isinstance(node.get("C"), list):
            cols = [c.get("N") for c in node["S"] if isinstance(c, dict)]
            if "M0" in cols:
                idx = cols.index("M0")
                if idx < len(node["C"]) and isinstance(node["C"][idx], str):
                    s = node["C"][idx].strip()
                    if s:
                        out.append(s)
    return out


def pick_best_title(payloads):
    candidates = collect_candidate_titles(payloads)

    seen = set()
    uniq = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            uniq.append(c)

    def score(s: str) -> int:
        t = s.strip().lower()
        if not t:
            return -10**9
        if "select one sale to view" in t:
            return -10**9
        if t in ("loading", "please wait"):
            return -10**9

        sc = 0
        if "sale no" in t:
            sc += 100
        if "report for" in t:
            sc += 80
        if "pgg" in t:
            sc += 60
        if "market report" in t:
            sc += 40
        if re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", t):
            sc += 30
        sc += min(len(t), 120)
        return sc

    if not uniq:
        return None, None, None, None, []

    best = max(uniq, key=score)

    sale_match = re.search(r"Sale\s*No\s*[:#]?\s*(\d+)", best, flags=re.IGNORECASE)
    date_match = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", best)
    saleyard = None
    m = re.search(r"Report\s+For\s+(.*?)\s+on\s*:", best, flags=re.IGNORECASE)
    if m:
        saleyard = m.group(1).strip()

    return (
        best,
        date_match.group(1) if date_match else None,
        saleyard,
        sale_match.group(1) if sale_match else None,
        uniq
    )


# ---------------- PLAYWRIGHT CAPTURE ----------------
async def capture_powerbi_payloads() -> tuple[list[dict], str]:
    captured_payloads = []
    sale_ui = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage"] if HEADLESS else None,
        )

        context = await browser.new_context(
            viewport={"width": 1366, "height": 900},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-NZ",
        )

        page = await context.new_page()

        async def on_response(resp):
            try:
                if resp.request.resource_type not in ("xhr", "fetch"):
                    return
                ct = (resp.headers.get("content-type") or "").lower()
                if "application/json" not in ct:
                    return
                body = await resp.json()
                if not (
                    isinstance(body, dict)
                    and isinstance(body.get("results"), list)
                    and body["results"]
                    and isinstance(safe_get(body, ["results", 0, "result", "data"]), dict)
                ):
                    return
                captured_payloads.append(body)
            except Exception:
                pass

        page.on("response", on_response)

        try:
            await page.goto(URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(4000)

            # Wait for iframe to exist
            await page.wait_for_selector("iframe", state="attached", timeout=90000)

            # Find PowerBI frame
            powerbi_frame = None
            for _ in range(90):
                for fr in page.frames:
                    if "powerbi" in (fr.url or "").lower():
                        powerbi_frame = fr
                        break
                if powerbi_frame:
                    break
                await page.wait_for_timeout(1000)

            if not powerbi_frame:
                await page.screenshot(path=str(DEBUG_DIR / "no_powerbi_frame.png"), full_page=True)
                (DEBUG_DIR / "no_powerbi_frame.html").write_text(await page.content(), encoding="utf-8")
                raise RuntimeError("Power BI iframe not found in CI")

            await powerbi_frame.wait_for_selector(".pivotTableCellNoWrap", timeout=90000)

            # 1) Click Date twice
            date_header = powerbi_frame.get_by_role("columnheader", name="Date")
            await date_header.click()
            await powerbi_frame.wait_for_timeout(WAIT_AFTER_SORT_MS)
            await date_header.click()
            await powerbi_frame.wait_for_timeout(WAIT_AFTER_SORT_MS)

            # 2) Click first sale
            first_sale = powerbi_frame.locator(".pivotTableCellNoWrap").first
            sale_ui = (await first_sale.inner_text()).strip()
            await first_sale.click()
            await powerbi_frame.wait_for_timeout(WAIT_AFTER_CLICK_MS)

        except Exception:
            try:
                await page.screenshot(path=str(DEBUG_DIR / "failure.png"), full_page=True)
                (DEBUG_DIR / "failure.html").write_text(await page.content(), encoding="utf-8")
            except Exception:
                pass
            raise
        finally:
            await browser.close()

    return captured_payloads, sale_ui or "unknown"


# ---------------- XLSX BUILD ----------------
def build_xlsx(captured_payloads: list[dict], sale_ui: str) -> Path:
    if len(captured_payloads) < MIN_XHR_EXPECTED:
        raise RuntimeError(f"No/low PowerBI payloads captured: {len(captured_payloads)}")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    raw_title, sale_date, saleyard, real_sale_no, title_candidates = pick_best_title(captured_payloads)

    sale_meta = {
        "sale_no": real_sale_no or sale_ui,
        "sale_key": f"{sale_ui}_{stamp}",
        "capture_id": stamp,
        "sale_date": sale_date,
        "saleyard": saleyard,
        "raw_title": raw_title,
    }

    debug_skips = []
    all_rows = []
    for i, payload in enumerate(captured_payloads):
        all_rows.extend(extract_rows_from_payload(payload, sale_meta, i, debug_skips))

    df_rows = pd.DataFrame(all_rows)

    # Ensure columns exist
    for k in KEYS:
        if k not in df_rows.columns:
            df_rows[k] = None
    for m in MEASURES:
        if m not in df_rows.columns:
            df_rows[m] = None

    df_rows["price_domain_conflict"] = df_rows[KG_COLS].notna().any(axis=1) & df_rows[HD_COLS].notna().any(axis=1)

    def first_non_null(series):
        for v in series:
            if pd.notna(v):
                return v
        return None

    df_master = (
        df_rows.drop_duplicates()
        .groupby(KEYS, dropna=False)[MEASURES + ["price_domain_conflict"]]
        .agg(first_non_null)
        .reset_index()
    )

    df_summary = df_master[df_master["table_type"] == "summary"].copy()
    df_breakdown = df_master[df_master["table_type"] == "breakdown"].copy()

    if not df_summary.empty:
        df_summary = df_summary.sort_values(["sale_date", "sale_no", "section", "row_level", "class"], na_position="last")
    if not df_breakdown.empty:
        df_breakdown = df_breakdown.sort_values(
            ["sale_date", "sale_no", "class", "row_level", "weight_from", "weight_to", "weight_range"],
            na_position="last"
        )

    out_path = OUT_DIR / f"AgOnline_{sale_meta['sale_no']}_{stamp}.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        pd.DataFrame([{
            **sale_meta,
            "payload_count": len(captured_payloads),
            "title_candidates_seen": len(title_candidates),
        }]).to_excel(writer, index=False, sheet_name="sales")

        df_summary.to_excel(writer, index=False, sheet_name="summary_rows")
        df_breakdown.to_excel(writer, index=False, sheet_name="breakdown_rows")

        # optional: include debug sheet only if there are skips (doesn't change your 3 core sheets)
        if debug_skips:
            pd.DataFrame(debug_skips).to_excel(writer, index=False, sheet_name="debug_skipped_rows")

    print("Wrote XLSX:", out_path)
    print("Payloads captured:", len(captured_payloads))
    print("Rows (summary):", len(df_summary))
    print("Rows (breakdown):", len(df_breakdown))

    return out_path


# ---------------- EMAIL ----------------
def email_file(path: Path):
    email_from = os.environ["EMAIL_FROM"]
    email_pass = os.environ["EMAIL_PASS"]

    msg = EmailMessage()
    msg["Subject"] = f"AgOnline XLSX (3 sheets) – {path.name}"
    msg["From"] = email_from
    msg["To"] = TO_EMAIL
    msg.set_content(
        "Attached: AgOnline XLSX export.\n\n"
        "Sheets:\n"
        " - sales\n"
        " - summary_rows\n"
        " - breakdown_rows\n"
    )

    msg.add_attachment(
        path.read_bytes(),
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=path.name,
    )

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(email_from, email_pass)
        server.send_message(msg)

    print("Email sent to:", TO_EMAIL)


# ---------------- MAIN ----------------
async def run():
    payloads, sale_ui = await capture_powerbi_payloads()
    xlsx_path = build_xlsx(payloads, sale_ui)
    email_file(xlsx_path)


if __name__ == "__main__":
    asyncio.run(run())
