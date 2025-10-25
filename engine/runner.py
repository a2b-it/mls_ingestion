
from __future__ import annotations
import argparse, yaml, json, time
from typing import Dict, Any, List
from .config import AppConfig, SourceConfig
from .http_client import send
from .mapping import map_json, map_xml
from .sinks import write_records

def run(config_path: str, mls: int, context: Dict[str, Any]) -> int:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    app = AppConfig(**raw)

    src = _get_source(app, mls)
    total = 0

    # basic pagination loop (page-based)
    if src.paginate and src.paginate.get("type") == "page":
        page = int(src.paginate.get("start", 1))
        max_pages = int(src.paginate.get("max_pages", 1))
        while page <= max_pages:
            ctx = {**context, "page": page}
            recs = _fetch_once(src, ctx)
            if not recs:
                break
            write_records(recs, src.sink.type, src.sink.path, "append" if (page > 1 and src.sink.mode=="overwrite") else src.sink.mode)
            total += len(recs)
            page += 1
            # backoff if configured?
    else:
        recs = _fetch_once(src, context)
        write_records(recs, src.sink.type, src.sink.path, src.sink.mode)
        total = len(recs)

    return total

def _fetch_once(src: SourceConfig, context: Dict[str, Any]) -> List[Dict[str,Any]]:
    resp = send(src.request, src.auth, context)
    resp.raise_for_status()
    if src.input_format == "json":
        doc = resp.json()
        return map_json(doc, src.mapping)
    else:
        content = resp.content
        return map_xml(content, src.mapping)

def _get_source(app: AppConfig, mls: int) -> SourceConfig:
    # 1) exact match
    for s in app.sources:
        if s.mls_id == mls:
            return s

    # 2) fallback to a default source with mls_id == 0
    for s in app.sources:
        if getattr(s, "mls_id", None) == 0:
            return s

    # 3) fallback to a source explicitly named 'default' (case-insensitive)
    for s in app.sources:
        if getattr(s, "name", "").lower() == "default":
            return s

    # 4) final fallback: return the first source (best-effort)
    if app.sources:
        return app.sources[0]

    raise SystemExit(f"No source found for MLS {mls}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True, help="Path to YAML config")
    p.add_argument("--mls", required=True, type=int, help="MLS id (per config)")
    p.add_argument("--since", required=False, help="ISO date for incremental fetch")
    p.add_argument("--log-file", required=False, help="Path to append request/response logs (JSONL)")
    args = p.parse_args()
    ctx = {}
    if args.since:
        ctx["since"] = args.since
    if args.log_file:
        ctx["log_file"] = args.log_file
    total = run(args.config, args.mls, ctx)
    print(f"Fetched {total} records.")
