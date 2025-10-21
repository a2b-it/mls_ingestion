
from __future__ import annotations
from typing import Any, Dict, Iterable, List
import jmespath
from lxml import etree
from .config import MappingSpec

def map_json(doc: Any, mapping: MappingSpec) -> List[Dict[str, Any]]:
    if mapping.root:
        items = jmespath.search(mapping.root, doc) or []
    else:
        items = [doc]
    out: List[Dict[str, Any]] = []
    for it in items:
        row = {}
        for f in mapping.fields:
            row[f.name] = jmespath.search(f.expr, it)
        out.append(row)
    return out

def map_xml(xml_bytes: bytes, mapping: MappingSpec) -> List[Dict[str, Any]]:
    root = etree.fromstring(xml_bytes)
    nsmap = root.nsmap
    if mapping.root:
        items = root.xpath(mapping.root, namespaces=nsmap)
    else:
        items = [root]
    out: List[Dict[str, Any]] = []
    for el in items:
        row = {}
        for f in mapping.fields:
            val = el.xpath(f.expr, namespaces=nsmap)
            if isinstance(val, list):
                if len(val) == 0:
                    row[f.name] = None
                elif len(val) == 1:
                    row[f.name] = _to_text(val[0])
                else:
                    row[f.name] = [_to_text(v) for v in val]
            else:
                row[f.name] = _to_text(val)
        out.append(row)
    return out

def _to_text(x):
    if hasattr(x, "text"):
        return x.text
    return x
