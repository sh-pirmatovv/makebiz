from __future__ import annotations

import json
from datetime import datetime


def norm(value: str) -> str:
    return (value or '').strip()


def bool_present(value: str) -> int:
    v = norm(value)
    return 0 if not v or v == '-' else 1


def score_company(row: dict) -> dict:
    # 1) Profile completeness 0..30
    keys = [
        'company_name', 'inn', 'registration_date', 'activity_status',
        'thsht', 'dbibt', 'ifut', 'address', 'region', 'director'
    ]
    filled = sum(1 for k in keys if bool_present(row.get(k, '-')))
    completeness_score = round((filled / len(keys)) * 30)

    # 2) Contacts 0..20
    contacts_score = 0
    if bool_present(row.get('phone', '-')):
        contacts_score += 12
    if bool_present(row.get('email', '-')):
        contacts_score += 8

    # 3) Activity status 0..20
    status = norm(row.get('activity_status', '-')).lower()
    if 'active' in status or 'mavjud' in status or 'faol' in status:
        activity_score = 20
    elif 'moderate' in status or 'qoniqarli' in status:
        activity_score = 12
    elif status == '-' or not status:
        activity_score = 8
    else:
        activity_score = 4

    # 4) Profile depth 0..20
    depth_fields = ['founders', 'tax_committee', 'large_taxpayer', 'category', 'district']
    depth_filled = sum(1 for k in depth_fields if bool_present(row.get(k, '-')))
    profile_depth_score = round((depth_filled / len(depth_fields)) * 20)

    # 5) Freshness proxy 0..10
    reg_date = norm(row.get('registration_date', '-'))
    freshness_score = 4
    if reg_date and reg_date != '-':
        try:
            year = int(reg_date.split('.')[-1])
            current_year = datetime.utcnow().year
            if current_year - year <= 2:
                freshness_score = 10
            elif current_year - year <= 6:
                freshness_score = 7
            else:
                freshness_score = 5
        except Exception:
            freshness_score = 4

    total = min(100, completeness_score + contacts_score + activity_score + profile_depth_score + freshness_score)

    if total >= 80:
        label = 'HIGH'
    elif total >= 65:
        label = 'MEDIUM'
    else:
        label = 'LOW'

    explain = {
        'completeness_score': completeness_score,
        'contacts_score': contacts_score,
        'activity_score': activity_score,
        'profile_depth_score': profile_depth_score,
        'freshness_score': freshness_score,
        'filled_profile_fields': filled,
    }

    return {
        'score': total,
        'score_label': label,
        'profile_completeness': round((filled / len(keys)) * 100, 2),
        'contact_valid': 1 if bool_present(row.get('phone', '-')) or bool_present(row.get('email', '-')) else 0,
        'score_explain': json.dumps(explain, ensure_ascii=False),
        **explain,
    }
