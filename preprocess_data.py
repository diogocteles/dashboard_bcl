#!/usr/bin/env python3
"""
preprocess_data.py — BioCollabs Dashboard Data Preprocessing Pipeline
Reads 45 Shopify order export CSVs (real Shipping Country per order)
-> Computes per-country monthly analytics, cohorts, retention
-> Writes data.js
"""
import csv, os, json, sys, re, urllib.parse
from collections import defaultdict
from datetime import datetime, date, timedelta

ORDERS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Orders')
ATC_CSV    = os.path.join(os.path.dirname(os.path.abspath(__file__)),
             'Sessions by landing page path - 2025-11-20 - 2026-02-20.csv')
OUTPUT_JS  = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data.js')
CTRY_KEYS  = ['ALL', 'US', 'GB', 'DE', 'NL', 'CA']
START_MONTH = '2020-03'
END_MONTH   = '2026-02'

def gen_months(start, end):
    months = []
    y, m = map(int, start.split('-'))
    ey, em = map(int, end.split('-'))
    while (y, m) <= (ey, em):
        months.append(f'{y:04d}-{m:02d}')
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return months

def months_offset(m1, m2):
    y1, mo1 = map(int, m1.split('-'))
    y2, mo2 = map(int, m2.split('-'))
    return (y2 - y1) * 12 + (mo2 - mo1)

def fnum(s):
    try:
        return float((s or '0').replace(',', ''))
    except:
        return 0.0

ALL_MONTHS    = gen_months(START_MONTH, END_MONTH)
COHORT_MONTHS = gen_months('2023-01', '2025-12')

def map_product_group(lname):
    """Map a Lineitem name to a canonical product group."""
    n = lname.lower()
    if 'detox' in n and '7' in n:   return 'Detox 7-Day'
    if 'detox' in n and '14' in n:  return 'Detox 14-Day'
    if 'detox' in n and '21' in n:  return 'Detox 21-Day'
    if 'detox' in n and '28' in n:  return 'Detox 28-Day'
    if 'body reset' in n or ('21 day' in n and 'detox' not in n): return '21-Day Body Reset'
    if 'calorie' in n:              return 'Calorie Blocker'
    if 'drinking' in n:             return 'Drinking Protocol'
    if 'sleep' in n:                return 'Better Sleep'
    if 'liver health' in n:         return 'Liver Health'
    if 'liver reset' in n:          return 'Liver Reset'
    if 'liver' in n:                return 'Liver Bundle'
    if 'brain' in n or 'energy' in n: return 'Brain & Energy'
    if 'digestion' in n or 'digestive' in n: return 'Digestion'
    if 'immune' in n:               return 'Immune Support'
    if 'tiredness' in n or 'tired' in n: return 'Tiredness'
    return 'Other'

# Step 1: Read all order exports, deduplicate by Name
print("Reading order files...")
files = sorted(f for f in os.listdir(ORDERS_DIR) if f.endswith('.csv'))
print(f"Found {len(files)} files")
orders = {}
order_product = {}  # order name → product group (from first non-empty lineitem)
total_rows = 0
for fn in files:
    path = os.path.join(ORDERS_DIR, fn)
    with open(path, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            name = (row.get('Name') or '').strip()
            if not name:
                continue
            # Capture first non-empty lineitem product per order (before dedup)
            if name not in order_product:
                lname = (row.get('Lineitem name') or '').strip()
                if lname:
                    order_product[name] = map_product_group(lname)
            if name in orders:
                continue
            # Skip bulk-imported historical orders (_E suffix = external system migration,
            # all stamped 2023-02-01 with wrong dates — causes false Feb 2023 spike)
            if '_E' in name:
                continue
            tags = (row.get('Tags') or '').strip()
            country_raw = (row.get('Shipping Country') or '').strip().upper()
            ctry = country_raw if country_raw in ('US', 'GB', 'DE', 'NL', 'CA') else 'OTHER'
            orders[name] = {
                'name': name,
                'email': (row.get('Email') or '').strip().lower(),
                'ctry': ctry,
                'created': (row.get('Created at') or '').strip(),
                'total': fnum(row.get('Total')),
                'subtotal': fnum(row.get('Subtotal')),
                'discount': fnum(row.get('Discount Amount')),
                'refunded': fnum(row.get('Refunded Amount')),
                'fin': (row.get('Financial Status') or '').strip().lower(),
                'tags': tags,
                'is_sub': name.startswith('#U') or 'subscription' in tags.lower(),
            }

print(f"Total rows read: {total_rows:,}")
print(f"Unique orders: {len(orders):,}")

# Step 2: Filter valid orders
valid = []
for o in orders.values():
    if o['fin'] in ('voided', 'pending', ''):
        continue
    month = o['created'][:7]
    if not month or month < START_MONTH or month > END_MONTH:
        continue
    o['month'] = month
    valid.append(o)

valid.sort(key=lambda o: o['created'])
print(f"Valid orders after filter: {len(valid):,}")

# Step 3: Customer history (new vs returning, sub sequences)
email_first = {}
email_sub_seq = defaultdict(int)

for o in valid:
    em = o['email']
    if em:
        o['is_new'] = em not in email_first
        if o['is_new']:
            email_first[em] = o['month']
        if o['is_sub']:
            email_sub_seq[em] += 1
            o['sub_seq'] = email_sub_seq[em]
        else:
            o['sub_seq'] = 0
    else:
        o['is_new'] = False
        o['sub_seq'] = 1 if o['is_sub'] else 0

# Step 4: Monthly aggregates per country
def empty_mo(m):
    return {'m': m, 'g': 0., 'n': 0., 'o': 0, 'nc': 0, 'rc': 0, 'd': 0., 'r': 0., 'sub': 0, 'ot': 0}

monthly = {c: {m: empty_mo(m) for m in ALL_MONTHS} for c in CTRY_KEYS}

for o in valid:
    m, ctry = o['month'], o['ctry']
    g = o['subtotal'] + o['discount']
    d = o['discount']
    r = o['refunded']
    n = o['subtotal'] - r
    dest = (['ALL'] + ([ctry] if ctry != 'OTHER' else []))
    for c in dest:
        mo = monthly[c][m]
        mo['g'] += g
        mo['n'] += n
        mo['o'] += 1
        mo['d'] += d
        mo['r'] += r
        if o['is_new']:
            mo['nc'] += 1
        else:
            mo['rc'] += 1
        if o['is_sub']:
            mo['sub'] += 1
        else:
            mo['ot'] += 1

country_monthly = {}
for c in CTRY_KEYS:
    arr = []
    for m in ALL_MONTHS:
        mo = monthly[c][m]
        arr.append({
            'm': m, 'g': round(mo['g']), 'n': round(mo['n']), 'o': mo['o'],
            'nc': mo['nc'], 'rc': mo['rc'], 'd': round(mo['d']), 'r': round(mo['r']),
            'sub': mo['sub'], 'ot': mo['ot']
        })
    country_monthly[c] = arr

# Step 5: Sub retention funnel
email_sub_info = {}
for o in valid:
    em = o.get('email', '')
    if not em or not o['is_sub'] or o['sub_seq'] == 0:
        continue
    if em not in email_sub_info:
        email_sub_info[em] = {'ctry': o['ctry'], 'max_seq': 0}
    email_sub_info[em]['max_seq'] = max(email_sub_info[em]['max_seq'], o['sub_seq'])

sub_ret = {c: [0] * 8 for c in CTRY_KEYS}
for em, info in email_sub_info.items():
    ctry, max_s = info['ctry'], info['max_seq']
    dest = ['ALL'] + ([ctry] if ctry != 'OTHER' else [])
    for c in dest:
        for i in range(min(max_s, 8)):
            sub_ret[c][i] += 1

# Step 6: Sub donut
sub_donut = {}
for c in CTRY_KEYS:
    sub_donut[c] = {
        'sub': sum(d['sub'] for d in country_monthly[c]),
        'ot':  sum(d['ot']  for d in country_monthly[c])
    }

# Step 7: Cohort analysis (2023-01 to 2025-12)
email_cohort = {}
for o in valid:
    em = o.get('email', '')
    if not em or not o['is_sub'] or o['sub_seq'] != 1:
        continue
    if '2023-01' <= o['month'] <= '2025-12':
        email_cohort[em] = {'cohort': o['month'], 'ctry': o['ctry']}

cohort_rev = {c: defaultdict(lambda: [0.] * 13) for c in CTRY_KEYS}
cohort_ord = {c: defaultdict(lambda: [0] * 13)  for c in CTRY_KEYS}

for o in valid:
    em = o.get('email', '')
    if not em or not o['is_sub']:
        continue
    info = email_cohort.get(em)
    if not info:
        continue
    offset = months_offset(info['cohort'], o['month'])
    if offset < 0 or offset > 12:
        continue
    ctry = info['ctry']
    g = o['subtotal'] + o['discount']
    dest = ['ALL'] + ([ctry] if ctry != 'OTHER' else [])
    for c in dest:
        cohort_rev[c][info['cohort']][offset] += g
        cohort_ord[c][info['cohort']][offset] += 1

country_cohort_rev = {}
country_cohort_ord = {}
for c in CTRY_KEYS:
    country_cohort_rev[c] = {m: [round(v) for v in cohort_rev[c].get(m, [0.] * 13)] for m in COHORT_MONTHS}
    country_cohort_ord[c] = {m: list(cohort_ord[c].get(m, [0] * 13)) for m in COHORT_MONTHS}

# Step 8: Country totals for KPI cards
FLAGS = {'ALL': '🌍', 'US': '🇺🇸', 'GB': '🇬🇧', 'DE': '🇩🇪', 'NL': '🇳🇱', 'CA': '🇨🇦'}
NAMES = {'ALL': 'All Markets', 'US': 'United States', 'GB': 'United Kingdom', 'DE': 'Germany', 'NL': 'Netherlands', 'CA': 'Canada'}
country_totals = {}
for c in CTRY_KEYS:
    arr = country_monthly[c]
    gross = sum(d['g'] for d in arr)
    net   = sum(d['n'] for d in arr)
    orders_ = sum(d['o'] for d in arr)
    nc    = sum(d['nc'] for d in arr)
    rc    = sum(d['rc'] for d in arr)
    disc  = sum(d['d'] for d in arr)
    ret   = sum(d['r'] for d in arr)
    aov   = gross / orders_ if orders_ else 0
    rr    = rc / (nc + rc) * 100 if (nc + rc) else 0
    dr    = disc / gross * 100 if gross else 0
    rtr   = ret / gross * 100 if gross else 0
    country_totals[c] = {
        'flag': FLAGS[c], 'name': NAMES[c],
        'gross': round(gross, 2), 'net': round(net, 2),
        'orders': orders_, 'nc': nc, 'rc': rc,
        'aov': round(aov, 2), 'rr': round(rr, 1), 'dr': round(dr, 2), 'rtr': round(rtr, 2),
        'sub': sub_donut[c]['sub'], 'ot': sub_donut[c]['ot'],
    }

# Print summary
for c in CTRY_KEYS:
    t = country_totals[c]
    print(f"  {c}: gross=${t['gross']:,.0f}  orders={t['orders']:,}  sub={t['sub']:,}")

# Step 9: Weekly channel data (18 months, Aug 2024 – Feb 2026)
print("\nBuilding weekly channel data...")
UTM_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Orders by order name.csv')
utm_by_name = {}
if os.path.exists(UTM_FILE):
    with open(UTM_FILE, encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            name = (row.get('Order name') or '').strip()
            med  = (row.get('Order UTM medium') or '').strip().lower()
            if name:
                utm_by_name[name] = med
    print(f"  UTM lookup built: {len(utm_by_name):,} entries")
else:
    print("  WARNING: Orders by order name.csv not found, skipping weekly channel data")

def classify_chan(med):
    if med in ('paid', 'cpc', 'paidsocial', 'social paid'):
        return 'paid'
    if med == 'email':
        return 'email'
    if med in ('sms', 'text'):
        return 'sms'
    if med == 'flow':
        return 'flow'
    return 'none'

CHAN_KEYS_W = ['paid', 'email', 'sms', 'flow', 'none']

def week_monday(d):
    return d - timedelta(days=d.weekday())

WEEKLY_START_DATE = date(2024, 8, 5)
WEEKLY_END_DATE   = date(2026, 2, 16)

weekly_acc = defaultdict(lambda: {c: {'o': 0, 'r': 0} for c in CHAN_KEYS_W})

for o in valid:
    try:
        d = date.fromisoformat(o['created'][:10])
    except ValueError:
        continue
    if d < WEEKLY_START_DATE or d > WEEKLY_END_DATE + timedelta(days=6):
        continue
    ws = week_monday(d)
    if ws < WEEKLY_START_DATE:
        continue
    chan = classify_chan(utm_by_name.get(o['name'], ''))
    g    = o['subtotal'] + o['discount']
    weekly_acc[ws][chan]['o'] += 1
    weekly_acc[ws][chan]['r'] += g

# Build sorted list (most-recent first)
ws = WEEKLY_START_DATE
all_weeks = []
while ws <= WEEKLY_END_DATE:
    entry = {'w': ws.isoformat()}
    data  = weekly_acc.get(ws, {c: {'o': 0, 'r': 0} for c in CHAN_KEYS_W})
    for c in CHAN_KEYS_W:
        entry[c] = {'o': data[c]['o'], 'r': round(data[c]['r'])}
    all_weeks.append(entry)
    ws += timedelta(weeks=1)

all_weeks.reverse()  # most-recent first
print(f"  Weekly rows: {len(all_weeks)}")

# Step 10: Weekly CVR + sessions by channel (from referrer source CSV, full range)
print("\nBuilding weekly CVR / sessions by channel...")
REF_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        'Conversion rate by referrer source - 2024-02-18 - 2026-02-18.csv')
ref_agg = defaultdict(lambda: {'sess': 0, 'comp': 0})
if os.path.exists(REF_FILE):
    with open(REF_FILE, encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            wk  = (row.get('Week') or '').strip()
            utm = (row.get('UTM medium') or '').strip().lower()
            chan = classify_chan(utm)
            s = int(row.get('Sessions') or 0)
            c = int(row.get('Sessions that completed checkout') or 0)
            ref_agg[(wk, chan)]['sess'] += s
            ref_agg[(wk, chan)]['comp'] += c
    print(f"  Referrer CSV rows aggregated for {len({k[0] for k in ref_agg})} weeks")
else:
    print("  WARNING: Referrer source CSV not found, skipping")

# Attach CVR + sessions to each weekly entry
for entry in all_weeks:
    w = entry['w']
    for c in CHAN_KEYS_W:
        d = ref_agg.get((w, c), {'sess': 0, 'comp': 0})
        entry[c]['s'] = d['sess']
        entry[c]['cvr'] = round(d['comp'] / d['sess'] * 100, 2) if d['sess'] > 0 else 0

# Step 11: SKU cohort — group customers by first-subscription product, track M+0..M+12
print("\nBuilding SKU cohort...")
email_first_product = {}  # email → product group of their first subscription order
for o in valid:  # valid is sorted by created
    em = o.get('email', '')
    if not em or not o['is_sub'] or o['sub_seq'] != 1:
        continue
    if em not in email_first_product:
        grp = order_product.get(o['name'], 'Other')
        email_first_product[em] = grp

sku_cohort_rev = defaultdict(lambda: [0.] * 13)
sku_cohort_ord = defaultdict(lambda: [0]  * 13)
for o in valid:
    em = o.get('email', '')
    if not em or not o['is_sub'] or o['sub_seq'] < 1 or o['sub_seq'] > 13:
        continue
    grp = email_first_product.get(em)
    if not grp:
        continue
    offset = o['sub_seq'] - 1  # M+0 = 1st order, M+1 = 2nd, ...
    g = o['subtotal'] + o['discount']
    sku_cohort_rev[grp][offset] += g
    sku_cohort_ord[grp][offset] += 1

# Sort groups by M+0 order count descending
sku_groups_ordered = sorted(sku_cohort_rev.keys(),
                            key=lambda g: sku_cohort_ord[g][0], reverse=True)
print(f"  Product groups: {len(sku_groups_ordered)}")
for g in sku_groups_ordered:
    m0_o = sku_cohort_ord[g][0]
    m1_r = round(sku_cohort_ord[g][1]/m0_o*100,1) if m0_o else 0
    print(f"    {g:<25s}  M+0={m0_o:6,}  M+1 retention={m1_r}%")

# Step 12: Product ATC (add-to-cart) from landing page sessions CSV
print("\nBuilding product ATC data...")

def map_slug_to_group(slug):
    s = slug.lower()
    if 'detox-7' in s or 'detox-week' in s or 'detox_week' in s: return 'Detox 7-Day'
    if 'detox-14' in s: return 'Detox 14-Day'
    if 'detox-21' in s or '21-days-detox' in s or 'mini-rehab' in s: return 'Detox 21-Day'
    if 'detox-28' in s: return 'Detox 28-Day'
    if '21-day-body-reset' in s or 'body-reset' in s: return '21-Day Body Reset'
    if 'calorie-bombs' in s or 'i-feel' in s: return 'Calorie Blocker'
    if 'drinking-protocol' in s: return 'Drinking Protocol'
    if 'dreaming' in s or 'sleep' in s: return 'Better Sleep'
    if 'liver-health' in s or 'fatty-liver' in s or 'fatty_liver' in s: return 'Liver Health'
    if 'liver-reset' in s: return 'Liver Reset'
    if 'liver' in s and 'health' not in s and 'reset' not in s and 'fatty' not in s: return 'Liver Bundle'
    if 'brain' in s or 'energy' in s or 'anxiety' in s: return 'Brain & Energy'
    if 'bloated' in s or 'digestive' in s or 'digestion' in s: return 'Digestion'
    if 'immunity' in s or 'immune' in s or 'achoo' in s or 'cactus-throat' in s or 'winter-fix' in s: return 'Immune Support'
    if 'knackered' in s or 'tiredness' in s: return 'Tiredness'
    return None

atc_grp_week = defaultdict(lambda: defaultdict(lambda: [0, 0]))  # [sessions, cart_adds]

if os.path.exists(ATC_CSV):
    with open(ATC_CSV, newline='', encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            path = urllib.parse.unquote(row['Landing page path']).lower()
            m = re.search(r'/products/([^/?#\s]+)', path)
            if not m: continue
            slug = m.group(1)
            grp = map_slug_to_group(slug)
            if not grp: continue
            week = row['Week'].strip('"')
            try:
                s = int(row['Sessions'])
                c = int(row['Sessions with cart additions'])
            except (ValueError, KeyError):
                continue
            atc_grp_week[grp][week][0] += s
            atc_grp_week[grp][week][1] += c
    print(f"  Loaded ATC data: {len(atc_grp_week)} product groups")
else:
    print(f"  WARNING: ATC CSV not found at {ATC_CSV}")

atc_weeks = sorted(set(w for g in atc_grp_week.values() for w in g))
# Only include groups with enough total sessions to be meaningful
atc_groups_ordered = sorted(
    [g for g in atc_grp_week if sum(v[0] for v in atc_grp_week[g].values()) >= 200],
    key=lambda g: -sum(v[0] for v in atc_grp_week[g].values())
)

# Step 13: Write data.js
def fmt_monthly(arr):
    parts = []
    for d in arr:
        parts.append(f"  {{m:'{d['m']}',g:{d['g']},n:{d['n']},o:{d['o']},nc:{d['nc']},rc:{d['rc']},d:{d['d']},r:{d['r']},sub:{d['sub']},ot:{d['ot']}}}")
    return '[\n' + ',\n'.join(parts) + '\n]'

ts = datetime.now().strftime('%Y-%m-%d %H:%M')
lines = [f'// Auto-generated by preprocess_data.py — {ts}', '']
lines.append('const COUNTRY_MONTHLY = {')
for c in CTRY_KEYS:
    lines.append(f'  {c}: {fmt_monthly(country_monthly[c])},')
lines.append('};\n')

lines.append('const COUNTRY_COHORT_REV = {')
for c in CTRY_KEYS:
    lines.append(f'  {c}: {{')
    for m in COHORT_MONTHS:
        lines.append(f"    '{m}':{json.dumps(country_cohort_rev[c][m])},")
    lines.append('  },')
lines.append('};\n')

lines.append('const COUNTRY_COHORT_ORD = {')
for c in CTRY_KEYS:
    lines.append(f'  {c}: {{')
    for m in COHORT_MONTHS:
        lines.append(f"    '{m}':{json.dumps(country_cohort_ord[c][m])},")
    lines.append('  },')
lines.append('};\n')

lines.append('const COUNTRY_SUB_RET = {')
for c in CTRY_KEYS:
    lines.append(f'  {c}: {json.dumps(sub_ret[c])},')
lines.append('};\n')

lines.append('const COUNTRY_SUB_DONUT = {')
for c in CTRY_KEYS:
    lines.append(f'  {c}: {json.dumps(sub_donut[c])},')
lines.append('};\n')

lines.append('const COUNTRY_TOTALS = {')
for c in CTRY_KEYS:
    lines.append(f'  {c}: {json.dumps(country_totals[c])},')
lines.append('};\n')

lines.append('const WEEKLY_CHAN = [')
for entry in all_weeks:
    lines.append(f"  {json.dumps(entry)},")
lines.append('];\n')

lines.append('const SKU_COHORT_REV = {')
for g in sku_groups_ordered:
    lines.append(f"  {json.dumps(g)}: {json.dumps([round(v) for v in sku_cohort_rev[g]])},")
lines.append('};\n')

lines.append('const SKU_COHORT_ORD = {')
for g in sku_groups_ordered:
    lines.append(f"  {json.dumps(g)}: {json.dumps(sku_cohort_ord[g])},")
lines.append('};\n')

lines.append(f'const PRODUCT_ATC_WEEKS = {json.dumps(atc_weeks)};\n')
lines.append('const PRODUCT_ATC = {')
for g in atc_groups_ordered:
    s_arr = [atc_grp_week[g].get(w, [0,0])[0] for w in atc_weeks]
    c_arr = [atc_grp_week[g].get(w, [0,0])[1] for w in atc_weeks]
    pct_arr = [round(c_arr[i]/s_arr[i]*100, 1) if s_arr[i] else None for i in range(len(atc_weeks))]
    lines.append(f"  {json.dumps(g)}: {{s:{json.dumps(s_arr)},c:{json.dumps(c_arr)},pct:{json.dumps(pct_arr)}}},")
lines.append('};\n')

with open(OUTPUT_JS, 'w') as f:
    f.write('\n'.join(lines))

kb = os.path.getsize(OUTPUT_JS) // 1024
print(f"\nWrote {OUTPUT_JS} ({kb} KB)")
print("Done!")
