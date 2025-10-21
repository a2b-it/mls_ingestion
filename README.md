# MLS Ingestion Engine (Config‑Driven JSON/XML Fetcher)

A lightweight, **config‑driven** Python engine to fetch data from heterogeneous HTTP sources (JSON or XML),
normalize fields via mappings (JMESPath / XPath), and write standardized outputs.

- HTTP via **httpx** with timeouts + **tenacity** retries
- Config + validation via **pydantic**
- JSON extraction via **jmespath**
- XML extraction via **lxml** XPath
- Request templating via **Jinja2**
- Pluggable sinks (ndjson, csv); easy to add Parquet/DB later
- Per‑MLS configuration: different URLs, headers, auth, params, input formats, and field mappings.

> **Run**
```bash
pip install httpx tenacity pydantic jmespath lxml Jinja2 pyyaml pandas
python -m engine.runner --config configs/mls_crmls.yaml --mls 14 --since 2025-01-01
python -m engine.runner --config configs/mls_nwmls.xml.yaml --mls 69 --since 2025-01-01
```
