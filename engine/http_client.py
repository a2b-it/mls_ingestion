
from __future__ import annotations
import httpx
from typing import Dict, Any, Optional
import json
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from .config import AuthConfig, RequestSpec
from jinja2 import Template

def build_auth_headers(auth: AuthConfig) -> Dict[str,str]:
    if auth.type == "none":
        return {}
    if auth.type == "basic":
        # Let httpx handle basic auth separately; still return {} here
        return {}
    if auth.type == "bearer" and auth.token:
        return {"Authorization": f"Bearer {auth.token}"}
    if auth.type == "api_key" and auth.header and auth.value:
        return {auth.header: auth.value}
    return {}

def format_with_jinja(template_obj: Any, context: Dict[str, Any]) -> Any:
    # Recursively render strings with Jinja2
    if isinstance(template_obj, dict):
        return {k: format_with_jinja(v, context) for k, v in template_obj.items()}
    if isinstance(template_obj, list):
        return [format_with_jinja(x, context) for x in template_obj]
    if isinstance(template_obj, str):
        return Template(template_obj).render(**context)
    return template_obj

@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=1, max=10),
    retry=retry_if_exception_type((httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError))
)
def send(spec: RequestSpec, auth: AuthConfig, context: Dict[str, Any]) -> httpx.Response:
    headers = {**spec.headers, **build_auth_headers(auth)}
    params = format_with_jinja(spec.params, context)
    url = format_with_jinja(spec.url, context)
    body = format_with_jinja(spec.body, context) if spec.body else None

    auth_tuple = None
    if auth.type == "basic" and auth.username and auth.password:
        auth_tuple = (auth.username, auth.password)

    log_file = context.get("log_file") if context else None

    # Prepare request record for logging (don't block execution on logging errors)
    request_record = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "request": {
            "method": spec.method,
            "url": url,
            "headers": headers,
            "params": params,
            "body": body,
        }
    }

    with httpx.Client(timeout=spec.timeout_seconds) as client:
        if spec.method == "GET":
            if auth_tuple:
                resp = client.get(url, headers=headers, params=params, auth=auth_tuple)
            else:
                resp = client.get(url, headers=headers, params=params)
        else:
            if auth_tuple:
                resp = client.post(url, headers=headers, params=params, json=body, auth=auth_tuple)
            else:
                resp = client.post(url, headers=headers, params=params, json=body)

    # Log response details (append JSON lines)
    if log_file:
        try:
            resp_text = None
            try:
                resp_text = resp.text
            except Exception:
                # fallback to bytes -> decode
                resp_text = resp.content.decode("utf-8", errors="replace")

            response_record = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "response": {
                    "status_code": resp.status_code,
                    "headers": dict(resp.headers),
                    "body": resp_text,
                }
            }

            # Merge request and response into single entry for easier tracing
            entry = {**request_record, **response_record}
            import os
            parent = os.path.dirname(log_file)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(log_file, "a", encoding="utf-8") as lf:
                lf.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            # Never allow logging failures to break the request flow
            pass

    return resp
