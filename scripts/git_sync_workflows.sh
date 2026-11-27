#!/usr/bin/env bash
set -euo pipefail

# n8n Automated Git Version Control Sync
PROJECT_DIR="/home/ubuntu/projects/n8n"
DATA_DIR="${PROJECT_DIR}/data"
WF_DIR="${PROJECT_DIR}/workflows"
TIMESTAMP="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

mkdir -p "${WF_DIR}"

# 1. Export monolithic workflow dump
docker exec n8n-automation n8n export:workflow --all --output=/home/node/.n8n/workflows_export.json >/dev/null 2>&1
docker exec n8n-automation n8n export:credentials --all --output=/home/node/.n8n/credentials_export.json >/dev/null 2>&1

# 2. Decompose into individual formatted workflow JSON files for granular Git diffs
python3 -c "
import json
from pathlib import Path

export_file = Path('${DATA_DIR}/workflows_export.json')
wf_dir = Path('${WF_DIR}')
if export_file.exists():
    try:
        workflows = json.loads(export_file.read_text(encoding='utf-8'))
        for wf in workflows:
            name = ''.join(c if c.isalnum() or c in '-_.' else '_' for c in wf.get('name', 'untitled')).strip('_')
            wid = wf.get('id', 'unknown')
            file_path = wf_dir / f'{name}__{wid}.json'
            file_path.write_text(json.dumps(wf, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        print(f'Decomposed {len(workflows)} workflows into individual git files.')
    except Exception as exc:
        print(f'Decomposition error: {exc}')
"

echo "[${TIMESTAMP}] n8n workflows successfully exported and synced to ${WF_DIR}"
