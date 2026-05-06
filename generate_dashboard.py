"""
DYU Clinic — Leads Dashboard Generator v2
Run locally or via GitHub Actions to regenerate index.html
"""

import requests, os, json
from datetime import datetime, timezone, timedelta
from pathlib import Path

META_TOKEN    = os.environ.get("META_ACCESS_TOKEN", "").strip()
AD_ACCOUNT_ID = os.environ.get("META_AD_ACCOUNT_ID", "1533474797458811").strip()
PAGE_ID       = "112698324956877"
IST           = timezone(timedelta(hours=5, minutes=30))
SCRIPT_DIR    = Path(__file__).parent
HIST_FILE     = SCRIPT_DIR / "historical_leads.json"
DASH_OUT      = SCRIPT_DIR / "index.html"

COLORS = {
    "Emsella":      "#1a5276",
    "Emsculpt Neo": "#784212",
    "Baby Spa":     "#6c3483",
    "LHR":          "#117a65",
    "Morpheus8":    "#943126",
    "IUI":          "#0e6655",
    "IVF":          "#1a6b4a",
    "B1G1":         "#6b4a1a",
}

# Each campaign: display name, keywords to match against form names
CAMPAIGNS = [
    {"name": "Emsella",      "keywords": ["emsella", "kegel"]},
    {"name": "Emsculpt Neo", "keywords": ["emsculpt", "emscutpt", "neo", "hifem"]},
    {"name": "Baby Spa",     "keywords": ["baby", "spa"]},
    {"name": "Morpheus8",    "keywords": ["morpheus"]},
    {"name": "LHR",          "keywords": ["laser", "lhr", "hair removal"]},
    {"name": "IUI",          "keywords": ["iui"]},
    {"name": "IVF",          "keywords": ["ivf", "fertility", "camp"]},
    {"name": "B1G1",         "keywords": ["b1g1", "women's day", "valentine", "bridal", "women's day"]},
]
CAMPAIGN_ORDER = [c["name"] for c in CAMPAIGNS]
SHEET_URL = "https://script.google.com/macros/s/AKfycbxqOBYfrlJfx_OM6HJgmsaOjewOIf29gHuRAO1CwSrHv-iOGvWXfGoa3wlQUmaWFb2v/exec"


def fmt_date(s):
    try:
        dt = datetime.fromisoformat(s.replace("+0000", "+00:00"))
        return dt.astimezone(IST).strftime("%d %b %Y, %I:%M %p")
    except Exception:
        return s

def iso_to_ts(s):
    try:
        dt = datetime.fromisoformat(s.replace("+0000", "+00:00"))
        return dt.astimezone(IST).strftime("%Y-%m-%d")
    except Exception:
        return ""

def tx_key(t):
    return t.lower().replace(" ", "_").replace("/", "_")

def infer_campaign(name):
    n = name.lower()
    for c in CAMPAIGNS:
        if any(k in n for k in c["keywords"]):
            return c["name"]
    return None


def get_page_token():
    r = requests.get("https://graph.facebook.com/v21.0/me/accounts",
        params={"access_token": META_TOKEN, "fields": "id,name,access_token"}).json()
    for acc in r.get("data", []):
        if acc["id"] == PAGE_ID:
            return acc["access_token"]
    return META_TOKEN


def fetch_page_forms(page_token):
    forms, url = [], f"https://graph.facebook.com/v21.0/{PAGE_ID}/leadgen_forms"
    params = {"access_token": page_token, "fields": "id,name,status,leads_count", "limit": 100}
    while url:
        resp = requests.get(url, params=params).json()
        if "error" in resp:
            print(f"  Form fetch error: {resp['error']['message']}")
            return []
        forms.extend(resp.get("data", []))
        url, params = resp.get("paging", {}).get("next"), {}
    print(f"  Found {len(forms)} lead forms")
    return forms


def fetch_leads_for_form(form_id, page_token):
    r = requests.get(f"https://graph.facebook.com/v21.0/{form_id}",
        params={"access_token": page_token, "fields": "questions"}).json()
    label_map = {"full_name": "Name", "phone_number": "Phone", "email": "Email"}
    value_map, q_order = {}, ["Name", "Phone", "Email"]
    for q in r.get("questions", []):
        key   = q.get("key", "")
        label = label_map.get(key, q.get("label", key))
        if key not in label_map: label_map[key] = label
        if label not in q_order: q_order.append(label)
        if q.get("options"): value_map[key] = {o["key"]: o["value"] for o in q["options"]}

    leads, url = [], f"https://graph.facebook.com/v21.0/{form_id}/leads"
    params = {"access_token": page_token, "fields": "created_time,field_data,id", "limit": 100}
    while url:
        resp = requests.get(url, params=params).json()
        if "error" in resp:
            return [], q_order
        for lead in resp.get("data", []):
            row = {"id": lead["id"], "date": fmt_date(lead["created_time"]), "date_ts": iso_to_ts(lead["created_time"])}
            for field in lead.get("field_data", []):
                col = label_map.get(field["name"], field["name"])
                raw = field["values"][0] if field.get("values") else ""
                row[col] = value_map.get(field["name"], {}).get(raw, raw)
            leads.append(row)
        url, params = resp.get("paging", {}).get("next"), {}
    return leads, q_order


def fetch_campaign_insights_monthly():
    """Returns dict: campaign -> {YYYY-MM -> {spend, impressions, meta_leads, cpl}}
    Covers last 12 months. One API call per campaign using time_increment=monthly."""
    today    = datetime.now(IST)
    since    = (today.replace(day=1) - timedelta(days=335)).strftime("%Y-%m-%d")
    until    = today.strftime("%Y-%m-%d")

    url    = f"https://graph.facebook.com/v21.0/act_{AD_ACCOUNT_ID}/campaigns"
    params = {"access_token": META_TOKEN, "fields": "id,name,effective_status", "limit": 100}
    campaigns = []
    while url:
        resp = requests.get(url, params=params).json()
        if "error" in resp:
            return {}
        campaigns.extend(resp.get("data", []))
        url, params = resp.get("paging", {}).get("next"), {}

    monthly = {}  # campaign -> {YYYY-MM -> {spend, impressions, meta_leads}}

    for c in campaigns:
        campaign = infer_campaign(c["name"])
        if not campaign:
            continue
        resp = requests.get(
            f"https://graph.facebook.com/v21.0/{c['id']}/insights",
            params={
                "access_token":   META_TOKEN,
                "time_range":     json.dumps({"since": since, "until": until}),
                "time_increment": "monthly",
                "fields":         "spend,impressions,actions,date_start",
                "limit":          50,
            }
        ).json()
        for row in resp.get("data", []):
            spend = float(row.get("spend", 0))
            if spend == 0:
                continue
            month_key  = row["date_start"][:7]  # YYYY-MM
            actions    = row.get("actions", [])
            meta_leads = next((int(a["value"]) for a in actions if a["action_type"] == "lead"), 0)
            m = monthly.setdefault(campaign, {}).setdefault(month_key, {"spend": 0.0, "impressions": 0, "meta_leads": 0})
            m["spend"]       += spend
            m["impressions"] += int(row.get("impressions", 0))
            m["meta_leads"]  += meta_leads

    # Compute CPL per cell
    for camp in monthly.values():
        for m in camp.values():
            m["cpl"]   = round(m["spend"] / m["meta_leads"], 0) if m["meta_leads"] > 0 else 0
            m["spend"] = round(m["spend"], 0)

    return monthly


def load_historical():
    return json.loads(HIST_FILE.read_text()) if HIST_FILE.exists() else []

def save_historical(leads):
    HIST_FILE.write_text(json.dumps(leads, ensure_ascii=False, indent=2))

def fetch_all_data():
    all_leads, seen = [], set()
    for l in load_historical():
        if l.get("id") and l["id"] not in seen:
            seen.add(l["id"])
            all_leads.append(l)
    page_token = get_page_token()
    for f in fetch_page_forms(page_token):
        treatment = infer_campaign(f.get("name", ""))
        print(f"  '{f.get('name')}' → {treatment}")
        leads, _ = fetch_leads_for_form(f["id"], page_token)
        print(f"    {len(leads)} leads")
        if not treatment:
            continue
        for lead in leads:
            lead["_form_name"] = f.get("name", f["id"])
            lead["_treatment"] = treatment
            if lead.get("id") and lead["id"] not in seen:
                seen.add(lead["id"])
                all_leads.append(lead)
    save_historical(all_leads)
    return all_leads


def generate(all_leads, insights):
    now_str   = datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST")
    cur_month = datetime.now(IST).strftime("%Y-%m")
    today_str = datetime.now(IST).strftime("%Y-%m-%d")

    for lead in all_leads:
        p = str(lead.get("Phone", ""))
        if p and not p.startswith("+") and p.isdigit() and len(p) >= 10:
            lead["Phone"] = "+" + p

    all_leads.sort(key=lambda l: l.get("date_ts", ""), reverse=True)

    lead_months    = sorted({l["date_ts"][:7] for l in all_leads if l.get("date_ts")}, reverse=True)
    insight_months = sorted({m for camp in insights.values() for m in camp}, reverse=True)
    all_months     = sorted(set(lead_months) | set(insight_months), reverse=True)

    dates    = [l["date_ts"] for l in all_leads if l.get("date_ts")]
    min_date = min(dates) if dates else ""
    max_date = today_str

    leads_json          = json.dumps(all_leads, ensure_ascii=False)
    colors_json         = json.dumps(COLORS)
    campaign_order_json = json.dumps(CAMPAIGN_ORDER)
    insights_json       = json.dumps(insights)
    months_json         = json.dumps(all_months)

    month_opts = "".join(
        f'<option value="{m}"{" selected" if m == cur_month else ""}>'
        f'{datetime.strptime(m, "%Y-%m").strftime("%b %Y")}</option>'
        for m in all_months
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>DYU Clinic — Leads Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f0f2f5;color:#1a1a2e;min-height:100vh}}
.header{{background:linear-gradient(135deg,#0d2137 0%,#1a3a5c 60%,#1a5276 100%);color:#fff;padding:22px 36px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px}}
.header h1{{font-size:1.55rem;font-weight:700;letter-spacing:-.3px}}
.header p{{opacity:.55;font-size:.8rem;margin-top:2px}}
.badge{{background:rgba(255,255,255,.13);padding:5px 14px;border-radius:20px;font-size:.72rem;white-space:nowrap}}
.wrap{{max-width:1320px;margin:0 auto;padding:24px 20px}}
.cards{{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:20px}}
.card{{background:#fff;border-radius:12px;padding:16px 20px;box-shadow:0 2px 8px rgba(0,0,0,.06);border-left:4px solid transparent}}
.card.c-leads{{border-color:#1a5276}}.card.c-spend{{border-color:#d4ac0d}}.card.c-cpl{{border-color:#117a65}}
.card.c-week{{border-color:#6c3483}}.card.c-today{{border-color:#943126}}
.card-val{{font-size:1.75rem;font-weight:700;color:#0d2137}}
.card-lbl{{font-size:.68rem;text-transform:uppercase;letter-spacing:.8px;color:#9ca3af;margin-top:2px;font-weight:600}}
.card-sub{{font-size:.7rem;color:#b0b8c4;margin-top:1px}}
.toolbar{{background:#fff;border-radius:12px;padding:14px 18px;box-shadow:0 2px 8px rgba(0,0,0,.06);margin-bottom:20px}}
.trow{{display:flex;flex-wrap:wrap;gap:12px;align-items:center}}
.trow+.trow{{margin-top:10px;padding-top:10px;border-top:1px solid #f3f4f6}}
.toolbar label{{font-size:.75rem;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:.5px}}
select.month-sel{{padding:7px 11px;border:1.5px solid #e5e7eb;border-radius:8px;font-size:.85rem;color:#1a1a2e;outline:none;background:#fff;cursor:pointer;min-width:130px}}
select.month-sel:focus{{border-color:#1a5276}}
input[type=date]{{padding:7px 11px;border:1.5px solid #e5e7eb;border-radius:8px;font-size:.83rem;color:#1a1a2e;outline:none}}
input[type=date]:focus{{border-color:#1a5276}}
.toggle-wrap{{display:flex;align-items:center;gap:6px;cursor:pointer}}
.toggle-wrap input[type=checkbox]{{width:16px;height:16px;cursor:pointer;accent-color:#1a5276}}
.toggle-label{{font-size:.78rem;color:#6b7280;font-weight:500}}
.sep{{flex:1}}.rc{{font-size:.78rem;color:#9ca3af;font-weight:500}}
.btn{{padding:8px 16px;border-radius:8px;font-size:.81rem;font-weight:600;cursor:pointer;border:none;transition:all .15s}}
.btn-xl{{background:#16a34a;color:#fff}}.btn-xl:hover{{background:#15803d}}
.btn-reset{{background:#f3f4f6;color:#374151}}.btn-reset:hover{{background:#e5e7eb}}
.nav{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:18px}}
.tab{{padding:8px 16px;border-radius:10px;border:2px solid #e5e7eb;background:#fff;color:#6b7280;font-size:.81rem;font-weight:600;cursor:pointer;transition:all .15s}}
.tab.active{{background:#0d2137;color:#fff;border-color:#0d2137}}
.tab:hover:not(.active){{border-color:#1a5276;color:#1a5276}}
.panel{{display:none}}.panel.visible{{display:block}}
.tx-bar{{border-radius:12px 12px 0 0;padding:16px 22px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:14px;color:#fff}}
.tx-name{{font-size:1.05rem;font-weight:700}}.tx-sub{{font-size:.75rem;opacity:.65;margin-top:2px}}
.metrics{{display:flex;gap:20px;flex-wrap:wrap}}.m-item{{text-align:center}}
.m-val{{font-size:1.2rem;font-weight:700}}.m-lbl{{font-size:.6rem;opacity:.7;text-transform:uppercase;letter-spacing:.4px;margin-top:1px}}
.insight-strip{{background:rgba(0,0,0,.18);padding:10px 22px;display:flex;gap:28px;flex-wrap:wrap}}
.is-item{{display:flex;flex-direction:column;align-items:center}}
.is-val{{font-size:.95rem;font-weight:700;color:#fff}}
.is-lbl{{font-size:.58rem;color:rgba(255,255,255,.6);text-transform:uppercase;letter-spacing:.4px;margin-top:1px}}
.tbl-card{{background:#fff;border-radius:0 0 12px 12px;box-shadow:0 2px 10px rgba(0,0,0,.07);margin-bottom:22px;overflow:hidden}}
.tbl-wrap{{overflow-x:auto}}
table{{width:100%;border-collapse:collapse;font-size:.81rem}}
th{{background:#0d2137;color:#fff;padding:10px 12px;text-align:left;font-size:.7rem;letter-spacing:.4px;white-space:nowrap;position:sticky;top:0;z-index:1}}
td{{padding:9px 12px;border-bottom:1px solid #f3f4f6;white-space:nowrap;max-width:240px;overflow:hidden;text-overflow:ellipsis;vertical-align:middle}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:#f8faff}}
tr.converted td{{background:#f0fdf4}}
tr.converted td:first-child{{border-left:3px solid #16a34a}}
.empty{{padding:40px;text-align:center;color:#9ca3af;font-size:.88rem}}
.remark-td{{white-space:normal!important;min-width:180px;max-width:280px}}
.remark-input{{width:100%;padding:5px 8px;border:1.5px solid #e5e7eb;border-radius:6px;font-size:.78rem;font-family:inherit;color:#1a1a2e;background:#fffdf0;resize:none;outline:none;transition:border-color .15s}}
.remark-input:focus{{border-color:#1a5276;background:#fff}}
.remark-input::placeholder{{color:#ccc}}
.conv-td{{text-align:center;width:80px}}
.conv-wrap{{display:flex;flex-direction:column;align-items:center;gap:3px}}
.conv-cb{{width:18px;height:18px;cursor:pointer;accent-color:#16a34a}}
.conv-lbl{{font-size:.6rem;color:#9ca3af}}
.footer{{text-align:center;padding:20px;color:#b0b8c4;font-size:.68rem}}
@media(max-width:768px){{
  .cards{{grid-template-columns:repeat(2,1fr)}}
  .metrics,.insight-strip{{gap:12px}}
  .trow{{flex-direction:column;align-items:flex-start}}
  .tx-bar{{flex-direction:column;align-items:flex-start}}
}}
</style>
</head>
<body>
<div class="header">
  <div><h1>DYU Clinic</h1><p>Meta Ads — Leads Dashboard</p></div>
  <div class="badge">Updated {now_str}</div>
</div>
<div class="wrap">
  <div class="cards">
    <div class="card c-leads">
      <div class="card-val" id="card-leads">—</div>
      <div class="card-lbl">Leads</div>
      <div class="card-sub" id="card-leads-sub">this month</div>
    </div>
    <div class="card c-spend">
      <div class="card-val" id="card-spend">—</div>
      <div class="card-lbl">Spend</div>
      <div class="card-sub" id="card-month-label">—</div>
    </div>
    <div class="card c-cpl">
      <div class="card-val" id="card-cpl">—</div>
      <div class="card-lbl">CPL</div>
      <div class="card-sub" id="card-cpl-label">—</div>
    </div>
    <div class="card c-week">
      <div class="card-val" id="card-week">—</div>
      <div class="card-lbl">This Week</div>
      <div class="card-sub">last 7 days</div>
    </div>
    <div class="card c-today">
      <div class="card-val" id="card-today">—</div>
      <div class="card-lbl">Today</div>
      <div class="card-sub">IST</div>
    </div>
  </div>
  <div class="toolbar">
    <div class="trow">
      <label>Month</label>
      <select class="month-sel" id="month-sel" onchange="changeMonth(this.value)">
        {month_opts}
      </select>
      <label style="margin-left:8px">or</label>
      <div class="toggle-wrap">
        <input type="checkbox" id="custom-range-cb" onchange="toggleCustomRange(this.checked)">
        <span class="toggle-label">Custom date range</span>
      </div>
      <div class="sep"></div>
      <span class="rc" id="rc"></span>
      <button class="btn btn-xl" onclick="dlExcel()">&#8595; Download Excel</button>
    </div>
    <div class="trow" id="custom-range-row" style="display:none">
      <label>From</label>
      <input type="date" id="df" value="{min_date}" min="{min_date}" max="{max_date}">
      <label>To</label>
      <input type="date" id="dt" value="{max_date}" min="{min_date}" max="{max_date}">
      <button class="btn btn-reset" onclick="resetDates()">Reset</button>
    </div>
  </div>
  <div class="nav" id="nav"></div>
  <div id="panels"></div>
</div>
<div class="footer">DYU Clinic × Gautami &nbsp;|&nbsp; Meta Ads only &nbsp;|&nbsp; Auto-refreshed every 2 hours</div>
<script>
const ALL_LEADS        = {leads_json};
const COLORS           = {colors_json};
const CAMPAIGN_ORDER   = {campaign_order_json};
const MONTHLY_INSIGHTS = {insights_json};
const ALL_MONTHS       = {months_json};
const META_COLS        = ['_form_name','_treatment','id','date','date_ts'];
const FIXED_COLS       = ['Name','Phone','Email'];
const SHEET_URL        = '{SHEET_URL}';
const sheetData        = {{}};

let curMonth      = '{cur_month}';
let dateRangeMode = false;
let curTab        = '';

function txKey(t){{return t.toLowerCase().replace(/ /g,'_').replace(/\\//g,'_');}}
function monthLabel(m){{
  if(!m)return'';
  const[y,mo]=m.split('-');
  return['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][parseInt(mo)-1]+' '+y;
}}

function getRemark(id){{return(sheetData[id]||{{}}).remarks||'';}}
function getConv(id){{const v=(sheetData[id]||{{}}).converted;return v===true||v==='true';}}
function getAmt(id){{return(sheetData[id]||{{}}).service_amount||'';}}

function setRemark(id,v){{if(!sheetData[id])sheetData[id]={{}};sheetData[id].remarks=v;syncToSheet(id);}}
function setConv(id,v){{if(!sheetData[id])sheetData[id]={{}};sheetData[id].converted=v;syncToSheet(id);updateRow(id);}}
function setAmt(id,v){{if(!sheetData[id])sheetData[id]={{}};sheetData[id].service_amount=v;syncToSheet(id);}}

let syncTimer={{}};
function syncToSheet(id){{
  clearTimeout(syncTimer[id]);
  syncTimer[id]=setTimeout(()=>{{
    const d=sheetData[id]||{{}};
    fetch(SHEET_URL+'?action=write'
      +'&lead_id='+encodeURIComponent(id)
      +'&remarks='+encodeURIComponent(d.remarks!==undefined?d.remarks:'')
      +'&converted='+encodeURIComponent(d.converted||false)
      +'&service_amount='+encodeURIComponent(d.service_amount!==undefined?d.service_amount:'')
    ).catch(()=>{{}});
  }},600);
}}

async function loadFromSheet(){{
  try{{
    const r=await fetch(SHEET_URL+'?action=read');
    const rows=await r.json();
    rows.forEach(row=>{{sheetData[row.lead_id]={{remarks:row.remarks,converted:row.converted,service_amount:row.service_amount}};}} );
  }}catch(e){{console.warn('Sheet sync failed',e);}}
  render();
}}

function getActiveCampaigns(month){{
  const withSpend=Object.keys(MONTHLY_INSIGHTS).filter(c=>{{
    const m=(MONTHLY_INSIGHTS[c]||{{}})[month];return m&&m.spend>0;
  }});
  const withLeads=[...new Set(ALL_LEADS.filter(l=>(l.date_ts||'').startsWith(month)).map(l=>l._treatment).filter(Boolean))];
  const union=new Set([...withSpend,...withLeads]);
  return CAMPAIGN_ORDER.filter(c=>union.has(c));
}}

function getMonthLeads(month){{return ALL_LEADS.filter(l=>(l.date_ts||'').startsWith(month));}}

function getDateRangeLeads(){{
  const f=document.getElementById('df').value,t=document.getElementById('dt').value;
  return ALL_LEADS.filter(l=>{{const d=l.date_ts||'';return(!f||d>=f)&&(!t||d<=t);}});
}}

function getCurrentLeads(){{return dateRangeMode?getDateRangeLeads():getMonthLeads(curMonth);}}

function changeMonth(m){{curMonth=m;render();}}

function toggleCustomRange(checked){{
  dateRangeMode=checked;
  document.getElementById('custom-range-row').style.display=checked?'flex':'none';
  render();
}}

function resetDates(){{
  const ds=ALL_LEADS.map(l=>l.date_ts).filter(Boolean).sort();
  document.getElementById('df').value=ds[0]||'';
  document.getElementById('dt').value=ds[ds.length-1]||'';
  render();
}}

document.getElementById('df').addEventListener('change',render);
document.getElementById('dt').addEventListener('change',render);

function showTab(key,el){{
  curTab=key;
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  el.classList.add('active');
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('visible'));
  const p=document.getElementById('panel-'+key);
  if(p)p.classList.add('visible');
}}

function extraCols(leads){{
  const skip=new Set([...META_COLS,...FIXED_COLS]);
  const cols=[];
  leads.forEach(l=>Object.keys(l).forEach(k=>{{if(!skip.has(k)&&!cols.includes(k))cols.push(k);}}));
  return cols;
}}

function buildTable(leads){{
  if(!leads.length)return'<div class="empty">No leads for this period.</div>';
  const extra=extraCols(leads);
  const dataCols=['Name','Phone','Email','Form','Submitted',...extra];
  const header=[...dataCols,'Service Amt (₹)','Remarks','Converted?'].map(c=>`<th>${{c}}</th>`).join('');
  const rows=leads.map(l=>{{
    const conv=getConv(l.id);
    const cells=dataCols.map(c=>{{
      if(c==='Form')     return`<td title="${{l._form_name||''}}">${{l._form_name||'—'}}</td>`;
      if(c==='Submitted')return`<td>${{l.date||'—'}}</td>`;
      return`<td title="${{(l[c]||'').toString().replace(/"/g,"'")}}">${{l[c]||'—'}}</td>`;
    }}).join('');
    const amtCell=`<td style="width:100px"><input type="number" min="0" style="width:90px;padding:5px 8px;border:1.5px solid #e5e7eb;border-radius:6px;font-size:.8rem;outline:none" value="${{getAmt(l.id)}}" placeholder="₹ 0" oninput="setAmt('${{l.id}}',this.value)" onfocus="this.style.borderColor='#1a5276'" onblur="this.style.borderColor='#e5e7eb'"></td>`;
    const remarkCell=`<td class="remark-td"><textarea class="remark-input" rows="2" placeholder="Add note…" oninput="setRemark('${{l.id}}',this.value)">${{getRemark(l.id)}}</textarea></td>`;
    const convCell=`<td class="conv-td"><div class="conv-wrap"><input type="checkbox" class="conv-cb" data-id="${{l.id}}" ${{conv?'checked':''}} onchange="toggleConv(this)"><span class="conv-lbl">${{conv?'✓':''}}</span></div></td>`;
    return`<tr class="${{conv?'converted':''}}" id="row-${{l.id}}">${{cells}}${{amtCell}}${{remarkCell}}${{convCell}}</tr>`;
  }}).join('');
  return`<div class="tbl-wrap"><table><thead><tr>${{header}}</tr></thead><tbody>${{rows}}</tbody></table></div>`;
}}

function updateRow(id){{
  const row=document.getElementById('row-'+id);
  if(!row)return;
  const conv=getConv(id);
  row.classList.toggle('converted',conv);
  const lbl=row.querySelector('.conv-lbl');
  if(lbl)lbl.textContent=conv?'✓':'';
}}

function toggleConv(cb){{setConv(cb.dataset.id,cb.checked);}}

function buildInsightStrip(campaign,month){{
  const d=(MONTHLY_INSIGHTS[campaign]||{{}})[month];
  if(!d)return'';
  return`<div class="insight-strip">
    <div class="is-item"><div class="is-val">₹${{Math.round(d.spend||0).toLocaleString('en-IN')}}</div><div class="is-lbl">Spend</div></div>
    <div class="is-item"><div class="is-val">₹${{d.cpl||'—'}}</div><div class="is-lbl">CPL</div></div>
    <div class="is-item"><div class="is-val">${{d.impressions?Math.round(d.impressions).toLocaleString('en-IN'):'—'}}</div><div class="is-lbl">Impressions</div></div>
    <div class="is-item"><div class="is-val">${{d.meta_leads||0}}</div><div class="is-lbl">Meta Leads</div></div>
  </div>`;
}}

function render(){{
  const activeCamps=getActiveCampaigns(curMonth);
  const leads=getCurrentLeads();
  const today=new Date().toISOString().slice(0,10);
  const week=new Date(Date.now()-7*864e5).toISOString().slice(0,10);

  // Summary cards
  let totalSpend=0,totalMetaLeads=0;
  activeCamps.forEach(c=>{{
    const m=(MONTHLY_INSIGHTS[c]||{{}})[curMonth]||{{}};
    totalSpend+=(m.spend||0);totalMetaLeads+=(m.meta_leads||0);
  }});
  const overallCPL=totalMetaLeads>0?Math.round(totalSpend/totalMetaLeads):0;
  const monthLeads=getMonthLeads(curMonth);
  document.getElementById('card-leads').textContent=monthLeads.length;
  document.getElementById('card-leads-sub').textContent=monthLabel(curMonth);
  document.getElementById('card-spend').textContent=totalSpend?'₹'+Math.round(totalSpend).toLocaleString('en-IN'):'—';
  document.getElementById('card-month-label').textContent=monthLabel(curMonth);
  document.getElementById('card-cpl').textContent=overallCPL?'₹'+overallCPL.toLocaleString('en-IN'):'—';
  document.getElementById('card-cpl-label').textContent=monthLabel(curMonth);
  document.getElementById('card-week').textContent=ALL_LEADS.filter(l=>(l.date_ts||'')>=week).length;
  document.getElementById('card-today').textContent=ALL_LEADS.filter(l=>(l.date_ts||'')>=today).length;

  // Tabs — preserve active tab if still in this month's campaigns
  if(!curTab||!activeCamps.map(txKey).includes(curTab)){{
    curTab=activeCamps.length?txKey(activeCamps[0]):'';
  }}
  document.getElementById('nav').innerHTML=activeCamps.map(c=>{{
    const key=txKey(c);
    return`<button class="tab ${{curTab===key?'active':''}}" onclick="showTab('${{key}}',this)">${{c}}</button>`;
  }}).join('');

  // Panels
  let html='';
  activeCamps.forEach(c=>{{
    const key=txKey(c);
    const color=COLORS[c]||'#1a5276';
    const campLeads=leads.filter(l=>l._treatment===c);
    const conv=campLeads.filter(l=>getConv(l.id)).length;
    html+=`<div class="panel ${{curTab===key?'visible':''}}" id="panel-${{key}}">
      <div class="tx-bar" style="background:${{color}}">
        <div><div class="tx-name">${{c}}</div><div class="tx-sub">${{campLeads.length}} leads${{dateRangeMode?' in range':' this month'}}</div></div>
        <div class="metrics">
          <div class="m-item"><div class="m-val">${{campLeads.length}}</div><div class="m-lbl">Leads</div></div>
          <div class="m-item"><div class="m-val">${{campLeads.filter(l=>(l.date_ts||'')>=week).length}}</div><div class="m-lbl">This Week</div></div>
          <div class="m-item"><div class="m-val">${{campLeads.filter(l=>(l.date_ts||'')>=today).length}}</div><div class="m-lbl">Today</div></div>
          <div class="m-item"><div class="m-val" style="color:#4ade80">${{conv}}</div><div class="m-lbl">Converted</div></div>
        </div>
      </div>
      ${{buildInsightStrip(c,curMonth)}}
      <div class="tbl-card">${{buildTable(campLeads)}}</div>
    </div>`;
  }});
  document.getElementById('panels').innerHTML=html;
  document.getElementById('rc').textContent=leads.length+' leads';
}}

function dlExcel(){{
  const wb=XLSX.utils.book_new();
  const activeCamps=getActiveCampaigns(curMonth);
  const leads=getCurrentLeads();
  activeCamps.forEach(c=>{{
    const campLeads=leads.filter(l=>l._treatment===c);
    if(!campLeads.length)return;
    const extra=extraCols(campLeads);
    const cols=['Name','Phone','Email','Form','Submitted',...extra,'Service Amt (₹)','Remarks','Converted'];
    const rows=campLeads.map(l=>{{
      const row={{}};
      cols.forEach(col=>{{
        if(col==='Form')                 row[col]=l._form_name||'';
        else if(col==='Submitted')       row[col]=l.date||'';
        else if(col==='Service Amt (₹)') row[col]=getAmt(l.id)||'';
        else if(col==='Remarks')         row[col]=getRemark(l.id);
        else if(col==='Converted')       row[col]=getConv(l.id)?'Yes':'No';
        else row[col]=l[col]||'';
      }});
      return row;
    }});
    const ws=XLSX.utils.json_to_sheet(rows,{{header:cols}});
    ws['!cols']=cols.map(()=>(({{wch:20}})));
    XLSX.utils.book_append_sheet(wb,ws,c.slice(0,31));
  }});
  const label=dateRangeMode
    ?`${{document.getElementById('df').value||'all'}}_to_${{document.getElementById('dt').value||'now'}}`
    :monthLabel(curMonth).replace(' ','_');
  XLSX.writeFile(wb,`DYU_Leads_${{label}}.xlsx`);
}}

loadFromSheet();
</script>
</body>
</html>"""


if __name__ == "__main__":
    if not META_TOKEN:
        print("ERROR: META_ACCESS_TOKEN not set.")
        raise SystemExit(1)

    print("Fetching campaign insights...", flush=True)
    insights = fetch_campaign_insights_monthly()

    print("Fetching leads...", flush=True)
    all_leads = fetch_all_data()
    print(f"Total leads: {len(all_leads)}", flush=True)

    html = generate(all_leads, insights)
    DASH_OUT.write_text(html)
    print(f"Dashboard saved → {DASH_OUT}", flush=True)
