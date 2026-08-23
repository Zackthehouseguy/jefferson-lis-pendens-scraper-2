#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path

ROOT = Path('reports/stress_50_live')
SRC = ROOT / 'extract_report.json'
OUT = ROOT / 'ai_input.json'


def main() -> int:
    payload = json.loads(SRC.read_text(encoding='utf-8'))
    records = payload.get('verified_open_unseen_records') or []
    compact = []
    for r in records:
        compact.append({
            'case_number': r.get('case_number'),
            'property_address': r.get('property_address'),
            'description_raw': r.get('description_raw'),
            'inspector_comments': r.get('inspector_comments') or [],
            'occupancies': r.get('recent_window_occupancies') or [],
            'violation_codes': r.get('recent_window_violation_codes') or [],
            'violation_descriptions': r.get('recent_window_descriptions') or [],
            'citation_event_count': r.get('citation_event_count') or 0,
            'citation_assessed_total': r.get('citation_assessed_total') or 0.0,
        })
    out = {
        'source_status': payload.get('status'),
        'record_count': len(compact),
        'generated_at_et': payload.get('generated_at_et'),
        'records': compact,
    }
    OUT.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding='utf-8')
    print(json.dumps({'record_count': len(compact), 'out': str(OUT)}))
    return 0 if len(compact) == 50 else 2

if __name__ == '__main__':
    raise SystemExit(main())
