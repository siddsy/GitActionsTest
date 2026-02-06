# ==========================================
# AgOnline PowerBI: 3 clicks -> capture sale XHR -> build clean master -> email XLSX
# Works locally + GitHub Actions
#
# What it does:
#  1) Opens https://www.agonline.co.nz/saleyard-results
#  2) Clicks Date header twice (PowerBI state reset)
#  3) Clicks first sale cell
#  4) Captures ALL XHR/FETCH PowerBI payloads triggered
#  5) Reconstructs rows (summary + breakdown) using your DSR decoder logic
#  6) Writes master-Raw.xlsx
#  7) Emails the XLSX as an attachment (SMTP via Gmail app password)
#
# Required env vars (GitHub Secrets):
#   EMAIL_FROM   (your gmail address)
#   EMAIL_PASS   (gmail app password, no spaces)
# Optional:
#   EMAIL_TO     (default: dunderhenlin@gmail.com)
# ==========================================

import os, json, re, smtplib
from datetime import datetime
from pathlib import Path
from email.message import EmailMessage

import pandas as pd
from playwright.sync_api import sync_playwright

# -----------------------
# CONFIG
# -----------------------
URL = "https://www.agonline.co.nz/saleyard-results"
EMAIL_TO = os.environ.get("EMAIL_TO", "dunderhenlin@gmail.com")
OUT_XLSX = "master-Raw.xlsx"

HEADLESS = True  # set False for local debugging
WAIT_AFTER_SORT_MS = 1200
WAIT_AFTER_CLICK_MS = 4500
MIN_XHR_EXPECTED = 3

MEASURES = [
    "qty_offered","qty_sold",
    "avg_price_kg","min_price_kg","max_price_kg",
    "avg_price_hd","min_price_hd","max_price_hd"
]
KG_COLS = ["avg_price_kg","min_price_kg","max_price_kg"]
HD_COLS = ["avg_price_hd","min_price_hd","max_price_hd"]

KEYS = [
    "sale_no","sale_key","capture_id","sale_date","saleyard","raw_title",
    "table_type","row_level","section","class",
    "weight_range","weight_from","weight_to"
]

# -----------------------
# Helpers
# -----------------------
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
        except:
            return x
    return x

def parse_weight_range(w):
    if not w or not isinstance(w, str) or "-" not in w:
        return None, None
    a, b = w.split("-", 1)
    try:
        return int(a.strip()), int(b.strip())
    except:
        return None, None

def get_value_dicts(dsr: dict):
    try:
        return dsr["DS"][0].get("ValueDicts", {}) or {}
    except:
        return {}

def decode_vdict(schema_entry: dict, raw_value, value_dicts: dict):
    dn = schema_entry.get("DN")
    if dn and dn in value_dicts and isinstance(raw_value, int):
        vd = value_dicts[dn]
        if 0 <= raw_value < len(vd):
            return vd[raw_value]
    return raw_value

def norm_measure(name: str):
    n = (name or "").lower()
    if "quantity offered" in n: return "qty_offered"
    if "quantity sold" in n: return "qty_sold"

    if "price/kg" in n:
        if "ave" in n or "avg" in n: return "avg_price_kg"
        if "min" in n: return "min_price_kg"
        if "max" in n: return "max_price_kg"

    if "price/hd" in n:
        if "ave" in n or "avg" in n: return "avg_price_hd"
        if "min" in n: return "min_price_hd"
        if "max" in n: return "max_price_hd"
    return None

def build_maps(descriptor: dict):
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

        if kind == 1 and isinstance(val, str) and val.startswith("G"):
            if "section" in lnm and "description" in lnm:
                dim_map[val] = "section"
            elif "weight" in lnm:
                dim_map[val] = "weight_range"
            elif lnm.endswith(".class") or "transactions.class" in lnm or "class" in lnm:
                dim_map[val] = "class"
            else:
                dim_map[val] = f"dim_{val}"

        if kind == 2 and isinstance(val, str) and val.startswith("M"):
            m = norm_measure(nm)
            if m:
                meas_map[val] = m
                for a in (s.get("Subtotal") or []):
                    if isinstance(a, str) and a.startswith("A"):
                        meas_map[a] = m

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

        if not schema:
            debug_list.append({
                "sale_no": sale_no, "payload_idx": payload_idx, "dm": dm_key, "rec_idx": rec_idx,
                "reason": "no_schema_available"
            })
            continue

        full = reconstruct_full(schema, rec, prev_full)
        if full is None:
            debug_list.append({
                "sale_no": sale_no, "payload_idx": payload_idx, "dm": dm_key, "rec_idx": rec_idx,
                "reason": "could_not_align_to_schema"
            })
            continue

        prev_full = full
        stream_state["prev_full"] = prev_full
        yield schema, full

def extract_rows_from_payload(payload: dict, sale_meta: dict, payload_idx: int, debug_list: list):
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
                            records,
                            stream_states[dm_key],
                            debug_list,
                            sale_no,
                            payload_idx,
                            dm_key=dm_key,
                            node_schema=node_schema
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

def extract_title_metadata_from_powerbi_payload(payload: dict):
    try:
        data = safe_get(payload, ["results", 0, "result", "data"])
        dsr = data.get("dsr", {})
        dm0 = safe_get(dsr, ["DS", 0, "PH", 0, "DM0"])
        if not isinstance(dm0, list) or not dm0:
            return None

        title_text = str(dm0[0].get("M0", "") or "").strip()
        if not title_text:
            return None

        sale_match = re.search(r"Sale\s*No\s*[:#]?\s*(\d+)", title_text, flags=re.IGNORECASE)
        date_match = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", title_text)

        return {
            "raw_text": title_text,
            "sale_no": sale_match.group(1) if sale_match else None,
            "date": date_match.group(1) if date_match else None
        }
    except Exception:
        return None

def find_title_like_info_from_payloads(payloads: list):
    best = None
    for p in payloads:
        t = extract_title_metadata_from_powerbi_payload(p)
        if t and t.get("raw_text"):
            best = t
            if t.get("sale_no"):
                break

    raw_title = best["raw_text"] if best else None
    sale_date = best.get("date") if best else None
    real_sale_no = best.get("sale_no") if best else None

    saleyard = None
    if raw_title:
        m = re.search(r"Report\s+For\s+(.*?)\s+on\s*:", raw_title, flags=re.IGNORECASE)
        saleyard = m.group(1).strip() if m else None

    return raw_title, sale_date, saleyard, real_sale_no

def send_email_with_attachment(subject: str, body: str, filepath: str):
    email_from = os.environ["EMAIL_FROM"]
    email_pass = os.environ["EMAIL_PASS"]

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = EMAIL_TO
    msg.set_content(body)

    with open(filepath, "rb") as f:
        data = f.read()

    msg.add_attachment(
        data,
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=os.path.basename(filepath),
    )

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(email_from, email_pass)
        server.send_message(msg)

# -----------------------
# Run: 3 clicks + capture payloads
# -----------------------
captured_payloads = []
sale_no_ui = None

with sync_playwright() as p:
    browser = p.chromium.launch(headless=HEADLESS)
    context = browser.new_context()
    page = context.new_page()

    # capture only PowerBI-shaped payloads
    def on_response(resp):
        try:
            if resp.request.resource_type not in ("xhr", "fetch"):
                return
            ct = (resp.headers.get("content-type") or "").lower()
            if "application/json" not in ct:
                return
            body = resp.json()
            if (
                isinstance(body, dict)
                and isinstance(body.get("results"), list)
                and body["results"]
                and isinstance(safe_get(body, ["results", 0, "result", "data"]), dict)
            ):
                captured_payloads.append(body)
        except Exception:
            pass

    page.on("response", on_response)

    page.goto(URL, timeout=60000)

    # find powerbi iframe
    page.wait_for_selector("iframe", timeout=60000)
    powerbi_frame = None
    for frame in page.frames:
        if "powerbi" in frame.url.lower():
            powerbi_frame = frame
            break
    if not powerbi_frame:
        browser.close()
        raise RuntimeError("Power BI iframe not found")

    # wait for pivot cells
    powerbi_frame.wait_for_selector(".pivotTableCellNoWrap", timeout=60000)

    # Click 1 & 2: Date header twice (reset)
    date_header = powerbi_frame.get_by_role("columnheader", name="Date")
    date_header.click()
    powerbi_frame.wait_for_timeout(WAIT_AFTER_SORT_MS)
    date_header.click()
    powerbi_frame.wait_for_timeout(WAIT_AFTER_SORT_MS)

    # Click 3: first sale
    first_sale = powerbi_frame.locator(".pivotTableCellNoWrap").first
    sale_no_ui = first_sale.inner_text().strip()
    first_sale.click()
    powerbi_frame.wait_for_timeout(WAIT_AFTER_CLICK_MS)

    browser.close()

if len(captured_payloads) < MIN_XHR_EXPECTED:
    raise RuntimeError(f"No/low PowerBI payloads captured: {len(captured_payloads)}")

# -----------------------
# Build meta + rows
# -----------------------
stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
sale_key = f"{sale_no_ui}_{stamp}"
raw_title, sale_date, saleyard, real_sale_no = find_title_like_info_from_payloads(captured_payloads)

sale_meta = {
    "sale_no": real_sale_no or sale_no_ui,
    "sale_key": sale_key,
    "capture_id": stamp,
    "sale_date": sale_date,
    "saleyard": saleyard,
    "raw_title": raw_title
}

debug_skips = []
all_rows = []
for idx, payload in enumerate(captured_payloads):
    all_rows.extend(extract_rows_from_payload(payload, sale_meta, idx, debug_skips))

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
    df_summary = df_summary.sort_values(["sale_date","sale_no","section","row_level","class"], na_position="last")
if not df_breakdown.empty:
    df_breakdown = df_breakdown.sort_values(
        ["sale_date","sale_no","class","row_level","weight_from","weight_to","weight_range"],
        na_position="last"
    )

# Write XLSX
with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
    pd.DataFrame([{
        **sale_meta,
        "payload_count": len(captured_payloads)
    }]).to_excel(writer, index=False, sheet_name="sales")
    df_summary.to_excel(writer, index=False, sheet_name="summary_rows")
    df_breakdown.to_excel(writer, index=False, sheet_name="breakdown_rows")
    if debug_skips:
        pd.DataFrame(debug_skips).to_excel(writer, index=False, sheet_name="debug_skipped_rows")

# -----------------------
# Email XLSX
# -----------------------
subject = f"AgOnline scrape: sale {sale_meta['sale_no']} ({sale_meta['capture_id']})"
body = (
    f"AgOnline scrape complete.\n\n"
    f"sale_no: {sale_meta['sale_no']}\n"
    f"sale_key: {sale_meta['sale_key']}\n"
    f"sale_date: {sale_meta['sale_date']}\n"
    f"saleyard: {sale_meta['saleyard']}\n"
    f"payloads captured: {len(captured_payloads)}\n"
    f"summary rows: {len(df_summary)}\n"
    f"breakdown rows: {len(df_breakdown)}\n"
)

send_email_with_attachment(subject, body, OUT_XLSX)

print("DONE. Email sent to:", EMAIL_TO)
print("Wrote:", OUT_XLSX)
