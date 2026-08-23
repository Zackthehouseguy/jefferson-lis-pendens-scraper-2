#!/usr/bin/env python3
from __future__ import annotations
import json
from collections import Counter
from pathlib import Path
ROOT=Path('reports/land_stress_50')
def main():
 d=json.loads((ROOT/'extract_report.json').read_text(encoding='utf-8'))
 excluded=d.get('excluded') or []
 raw_fail=d.get('raw_failures') or []
 ef=d.get('enrichment_failures') or []
 summary={
  'status':d.get('status'),'target':d.get('target'),'accepted':len(d.get('private_unseen_land_records') or []),
  'raw_target':d.get('raw_target'),'raw_runtime_seconds':d.get('raw_runtime_seconds'),
  'pm_features_fetched':d.get('pm_features_fetched'),'parent_groups_discovered':d.get('parent_groups_discovered'),
  'vacant_lot_parent_groups':d.get('vacant_lot_parent_groups'),'demolition_transition_watch_groups':d.get('demolition_transition_watch_groups'),
  'excluded_total':len(excluded),'excluded_reasons':dict(Counter(x.get('reason') for x in excluded)),
  'enrichment_failure_records':len(ef),'raw_failure_records':len(raw_fail),
  'raw_failure_stages':dict(Counter(x.get('stage') for x in raw_fail)),
  'sample_excluded':excluded[:15],'sample_raw_failures':raw_fail[:15]
 }
 (ROOT/'summary.json').write_text(json.dumps(summary,indent=2,ensure_ascii=False),encoding='utf-8')
 print(json.dumps(summary,indent=2,ensure_ascii=False))
 return 0
if __name__=='__main__':raise SystemExit(main())
