#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path

ROOT = Path('reports/stress_50_live')
SRC = ROOT / 'ai_input.json'


def main() -> int:
    payload = json.loads(SRC.read_text(encoding='utf-8'))
    rows = payload.get('records') or []
    if len(rows) != 50:
        raise RuntimeError(f'expected_50_records_got_{len(rows)}')
    for start in range(0, 50, 10):
        end = start + 10
        out = {
            'source_status': payload.get('source_status'),
            'generated_at_et': payload.get('generated_at_et'),
            'range': [start + 1, end],
            'records': rows[start:end],
        }
        path = ROOT / f'ai_input_{start+1:02d}_{end:02d}.json'
        path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding='utf-8')
    print('split 50 records into 5 chunks')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
