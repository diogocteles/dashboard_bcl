#!/usr/bin/env python3
"""
preprocess_data.py — BioCollabs Dashboard Data Preprocessing Pipeline
Reads 45 Shopify order export CSVs (real Shipping Country per order)
-> Computes per-country monthly analytics, cohorts, retention
-> Writes data.js
"""
import csv, os, json, sys
from collections import defaultdict
from datetime import datetime

ORDERS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Orders')
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

# Step 1: Read all order exports, deduplicate by Name
print("Reading order files...")
files = sorted(f for f in os.listdir(ORDERS_DIR) if f.endswith('.csv'))
print(f"Found {len(files)} files")
orders = {}
total_rows = 0
for fn in files:
    path = os.path.join(ORDERS_DIR, fn)
    with open(path, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            name = (row.get('Name') or '').strip()
            if not name or name in orders:
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

# Step 9: Write data.js
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

with open(OUTPUT_JS, 'w') as f:
    f.write('\n'.join(lines))

kb = os.path.getsize(OUTPUT_JS) // 1024
print(f"\nWrote {OUTPUT_JS} ({kb} KB)")
print("Done!")
