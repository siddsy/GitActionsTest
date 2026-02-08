import os
import re
import smtplib
from email.message import EmailMessage
from datetime import datetime

import pandas as pd
from playwright.sync_api import sync_playwright

URL = "https://www.agonline.co.nz/saleyard-results"

HEADLESS = True
WAIT_AFTER_SORT_MS = 1500
WAIT_AFTER_CLICK_MS = 9000
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
    if isinstance(x, (int, float)): return x
    if isinstance(x, str):
        try: return float(x)
        except: return x
    return x

def parse_weight_range(w):
    if isinstance(w, str) and "-" in w:
        try:
            a, b = w.split("-", 1)
            return int(a.strip()), int(b.strip())
        except:
            pass
    return None, None

def get_value_dicts(dsr):
    try:
        return dsr["DS"][0].get("ValueDicts", {}) or {}
    except:
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

        if kind == 1 and isinstance(val, str) and val.startswith("G"):
            if "section" in lnm and "description" in lnm:
                dim_map[val] = "section"
            elif "weight" in lnm:
                dim_map[val] = "weight_range"
            elif "class" in lnm:
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
                        stream_states.setdefault(dm_key, {})

                        is_subtotal = child_dm in subtotal_members

                        for schema, full in iter_decoded_records(records, stream_states[dm_key], debug_list, sale_no, payload_idx, dm_key, node_schema):
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

        sc = 0
        if "sale no" in t: sc += 100
        if "report for" in t: sc += 80
        if "pgg" in t: sc += 60
        if "market report" in t: sc += 40
        if re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", t): sc += 30
        sc += min(len(t), 120)
        return sc

    best = max(uniq, key=score) if uniq else None
    if not best:
        return None, None, None, None

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
        sale_match.group(1) if sale_match else None
    )

def send_email(subject: str, body: str, attachments: dict | None = None):
    from_addr = os.environ["EMAIL_FROM"]
    to_addr = os.environ["EMAIL_TO"]
    app_pass = os.environ["EMAIL_PASS"]

    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)

    # Attach CSVs in-memory (no files)
    if attachments:
        for filename, text in attachments.items():
            msg.add_attachment(
                text.encode("utf-8"),
                maintype="text",
                subtype="csv",
                filename=filename,
            )

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(from_addr, app_pass)
        smtp.send_message(msg)

def run_once():
    captured_payloads = []
    sale_ui = None

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
        )
        page = context.new_page()

        def on_response(resp):
            try:
                if resp.request.resource_type not in ("xhr", "fetch"):
                    return
                ct = (resp.headers.get("content-type") or "").lower()
                if "application/json" not in ct:
                    return
                body = resp.json()
                if not (
                    isinstance(body, dict)
                    and isinstance(body.get("results"), list)
                    and body["results"]
                    and isinstance(safe_get(body, ["results", 0, "result", "data"]), dict)
                ):
                    return
                captured_payloads.append(body)
            except:
                pass

        page.on("response", on_response)

        page.goto(URL, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_load_state("networkidle", timeout=90000)

        # Try to find iframe for up to 90s, otherwise dump debug artifacts
        try:
            page.wait_for_selector("iframe", timeout=90000)
        except Exception:
            # DEBUG OUTPUTS (for Actions artifacts)
            page.screenshot(path="debug_page.png", full_page=True)
            html = page.content()
            with open("debug_page.html", "w", encoding="utf-8") as f:
                f.write(html)

            # also log a short snippet to Actions
            title = page.title()
            print("DEBUG: page title =", title)
            print("DEBUG: first 500 chars of HTML:")
            print(html[:500])

            browser.close()
            raise RuntimeError("No iframe found. Saved debug_page.png and debug_page.html")

        # Find PowerBI iframe
        powerbi_frame = None
        for fr in page.frames:
            if "powerbi" in (fr.url or "").lower():
                powerbi_frame = fr
                break
        if not powerbi_frame:
            page.screenshot(path="debug_no_powerbi_frame.png", full_page=True)
            browser.close()
            raise RuntimeError("Iframe exists, but no PowerBI frame URL matched. Saved debug_no_powerbi_frame.png")

        powerbi_frame.wait_for_selector(".pivotTableCellNoWrap", timeout=90000)

        # 1) click Date twice
        date_header = powerbi_frame.get_by_role("columnheader", name="Date")
        date_header.click()
        powerbi_frame.wait_for_timeout(WAIT_AFTER_SORT_MS)
        date_header.click()
        powerbi_frame.wait_for_timeout(WAIT_AFTER_SORT_MS)

        # 2) click first sale
        first_sale = powerbi_frame.locator(".pivotTableCellNoWrap").first
        sale_ui = (first_sale.inner_text() or "").strip()
        first_sale.click()
        powerbi_frame.wait_for_timeout(WAIT_AFTER_CLICK_MS)

        browser.close()

    if len(captured_payloads) < MIN_XHR_EXPECTED:
        raise RuntimeError(f"Too few payloads captured: {len(captured_payloads)}")

    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    raw_title, sale_date, saleyard, real_sale_no = pick_best_title(captured_payloads)

    sale_no = real_sale_no or sale_ui or "unknown"
    sale_meta = {
        "sale_no": sale_no,
        "sale_key": f"{sale_no}_{stamp}",
        "capture_id": stamp,
        "sale_date": sale_date,
        "saleyard": saleyard,
        "raw_title": raw_title
    }

    debug_skips = []
    all_rows = []
    for i, payload in enumerate(captured_payloads):
        all_rows.extend(extract_rows_from_payload(payload, sale_meta, i, debug_skips))

    df_rows = pd.DataFrame(all_rows)
    if df_rows.empty:
        raise RuntimeError("Decoded rows empty (no data).")

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

    return sale_meta, len(captured_payloads), df_summary, df_breakdown

if __name__ == "__main__":
    sale_meta, payloads, df_summary, df_breakdown = run_once()

    # Convert to CSV strings (in memory)
    # Keep them reasonably sized: head if huge
    max_rows = 2500
    sum_out = df_summary.head(max_rows)
    brk_out = df_breakdown.head(max_rows)

    summary_csv = sum_out.to_csv(index=False)
    breakdown_csv = brk_out.to_csv(index=False)

    subject = f"AgOnline scrape OK — Sale {sale_meta['sale_no']} ({payloads} payloads)"
    body = (
        f"Scrape completed.\n\n"
        f"Sale: {sale_meta['sale_no']}\n"
        f"Saleyard: {sale_meta['saleyard']}\n"
        f"Date: {sale_meta['sale_date']}\n"
        f"Payloads: {payloads}\n"
        f"Summary rows: {len(df_summary)} (attached up to {max_rows})\n"
        f"Breakdown rows: {len(df_breakdown)} (attached up to {max_rows})\n\n"
        f"Raw title:\n{sale_meta['raw_title']}\n"
    )

    send_email(
        subject,
        body,
        attachments={
            f"summary_rows_{sale_meta['sale_no']}.csv": summary_csv,
            f"breakdown_rows_{sale_meta['sale_no']}.csv": breakdown_csv,
        }
    )

    print("EMAIL SENT:", subject)
