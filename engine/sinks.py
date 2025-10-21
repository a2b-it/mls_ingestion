
from __future__ import annotations
from typing import List, Dict, Any
import os, csv, json
import pandas as pd

def write_records(records: List[Dict[str, Any]], sink_type: str, path: str, mode: str = "overwrite"):
    if not records:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if sink_type == "ndjson":
        _write_ndjson(records, path, mode)
    elif sink_type == "csv":
        _write_csv(records, path, mode)
    else:
        raise ValueError(f"Unsupported sink type: {sink_type}")

def _write_ndjson(records, path, mode):
    write_mode = "w" if mode == "overwrite" else "a"
    with open(path, write_mode, encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def _write_csv(records, path, mode):
    df = pd.DataFrame(records)
    write_mode = "w" if mode == "overwrite" else "a"
    header = not (mode == "append" and os.path.exists(path))
    df.to_csv(path, index=False, mode=write_mode, header=header)
