
from __future__ import annotations
import argparse, yaml, json, time
from typing import Dict, Any, List, Optional, Tuple
import jmespath
from lxml import etree
from .config import AppConfig, SourceConfig
from .http_client import send
from .mapping import map_json, map_xml
from .sinks import write_records

def run(config_path: str, mls: int, context: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    """Run ingestion and return (total_records, metadata).

    metadata contains at minimum:
      - elapsed_seconds
      - sample (first record fields, if any)
    """
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    app = AppConfig(**raw)

    src = _get_source(app, mls)
    total = 0
    sample: Optional[Dict[str, Any]] = None
    last_resp = None
    start = time.time()

    # basic pagination loop (page-based)
    if src.paginate and src.paginate.get("type") == "page":
        page = int(src.paginate.get("start", 1))
        max_pages = int(src.paginate.get("max_pages", 1))
        while page <= max_pages:
            ctx = {**context, "page": page}
            recs, resp = _fetch_once(src, ctx)
            last_resp = resp
            if not recs:
                break
            if sample is None and len(recs) > 0:
                sample = recs[0]
            write_records(recs, src.sink.type, src.sink.path, "append" if (page > 1 and src.sink.mode=="overwrite") else src.sink.mode)
            total += len(recs)
            page += 1
            # backoff if configured?
    else:
        recs, resp = _fetch_once(src, context)
        last_resp = resp
        if recs and len(recs) > 0:
            sample = recs[0]
        write_records(recs, src.sink.type, src.sink.path, src.sink.mode)
        total = len(recs)

    elapsed = time.time() - start
    metadata = {
        "elapsed_seconds": elapsed,
        "sample": sample,
    }
    # If configured, extract response-level fields from the last HTTP response
    try:
        if getattr(src, "response", None) and last_resp is not None:
            metadata["response_fields"] = _extract_response_fields(last_resp, src.response)
    except Exception:
        # don't fail the run due to response extraction errors
        pass
    return total, metadata


def _extract_response_fields(resp: Any, response_spec) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    json_doc: Optional[Any] = None
    xml_root = None
    nsmap = None

    for f in response_spec.fields:
        src = getattr(f, "source", "json")
        expr = getattr(f, "expr", None)
        if src == "status":
            out[f.name] = getattr(resp, "status_code", None)
        elif src == "header":
            # headers are case-insensitive
            try:
                headers = {k.lower(): v for k, v in dict(resp.headers).items()}
                out[f.name] = headers.get((expr or "").lower())
            except Exception:
                out[f.name] = None
        elif src == "json":
            if json_doc is None:
                try:
                    json_doc = resp.json()
                except Exception:
                    json_doc = None
            if json_doc is None or not expr:
                out[f.name] = None
            else:
                try:
                    out[f.name] = jmespath.search(expr, json_doc)
                except Exception:
                    out[f.name] = None
        elif src == "xml":
            if xml_root is None:
                try:
                    xml_root = etree.fromstring(resp.content)
                    nsmap = xml_root.nsmap
                except Exception:
                    xml_root = None
            if xml_root is None or not expr:
                out[f.name] = None
            else:
                try:
                    val = xml_root.xpath(expr, namespaces=nsmap)
                    if isinstance(val, list):
                        if len(val) == 0:
                            out[f.name] = None
                        elif len(val) == 1:
                            v = val[0]
                            out[f.name] = v.text if hasattr(v, "text") else v
                        else:
                            out[f.name] = [ (v.text if hasattr(v, "text") else v) for v in val ]
                    else:
                        out[f.name] = val
                except Exception:
                    out[f.name] = None
        else:
            out[f.name] = None

    return out

def _fetch_once(src: SourceConfig, context: Dict[str, Any]) -> Tuple[List[Dict[str,Any]], Any]:
    resp = send(src.request, src.auth, context)
    resp.raise_for_status()
    if src.input_format == "json":
        doc = resp.json()
        return map_json(doc, src.mapping), resp
    else:
        content = resp.content
        return map_xml(content, src.mapping), resp

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
    result = run(args.config, args.mls, ctx)
    if isinstance(result, tuple):
        total = result[0]
    else:
        total = result
    print(f"Fetched {total} records.")
