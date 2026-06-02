#!/usr/bin/env python3
"""
validate.py — Validate CIFS scan/augment results in the dev OpenSearch.

Queries the index `pipeline-cifs-dsid-<suffix>` and reports document count plus
a sample of source_url / file_meta / connector_specific. With --require-augment
it asserts that documents carry the augment marker (`augment_done: true`, set by
the connector's buildAugmentedItem — see cifs-connector-be-go/internal/cifsscan/
augment.go:586), so you can gate the scan→augment sequence.

Targets the port-forwarded OpenSearch by default:
  kubectl -n "$NS" port-forward svc/opensearch 9200:9200 &
  python validate.py --suffix 001
  python validate.py --suffix 001 --require-augment

OpenSearch in dev may require auth/TLS — pass --user/--password (or
OPENSEARCH_USER / OPENSEARCH_PASSWORD) and --insecure for self-signed certs.
"""

import argparse
import json
import os
import sys
from typing import Any

import requests
from requests.auth import HTTPBasicAuth


DEFAULT_OS_URL = "http://localhost:9200"
AUGMENT_MARKER = "augment_done"


def find_key(obj: Any, key: str):
    """Recursively yield values for `key` anywhere in a nested dict/list.
    connector_specific may be indexed as a nested object or a stringified JSON
    blob depending on the archiver mapping, so we search defensively and also
    parse string values that look like JSON."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key:
                yield v
            yield from find_key(v, key)
    elif isinstance(obj, list):
        for v in obj:
            yield from find_key(v, key)
    elif isinstance(obj, str) and key in obj:
        try:
            yield from find_key(json.loads(obj), key)
        except (ValueError, TypeError):
            pass


def request(method: str, url: str, auth, verify: bool, **kw) -> requests.Response:
    return requests.request(method, url, auth=auth, verify=verify, timeout=30, **kw)


def main() -> int:
    p = argparse.ArgumentParser(description="Validate CIFS results in dev OpenSearch")
    p.add_argument("--suffix", default="001", help="Datasource id suffix (index = pipeline-cifs-dsid-<suffix>)")
    p.add_argument("--index", default="", help="Override full index name (ignores --suffix)")
    p.add_argument("--os-url", default=os.getenv("OPENSEARCH_URL") or DEFAULT_OS_URL,
                   help=f"OpenSearch base URL (default: {DEFAULT_OS_URL})")
    p.add_argument("--user", default=os.getenv("OPENSEARCH_USER") or "")
    p.add_argument("--password", default=os.getenv("OPENSEARCH_PASSWORD") or "")
    p.add_argument("--insecure", action="store_true", help="Skip TLS verification (self-signed certs)")
    p.add_argument("--show", type=int, default=3, help="How many sample docs to print (default: 3)")
    p.add_argument("--expect-count", type=int, default=0, help="Fail if doc count is below this")
    p.add_argument("--require-augment", action="store_true",
                   help=f"Fail unless every doc carries '{AUGMENT_MARKER}: true'")
    args = p.parse_args()

    index = args.index or f"pipeline-cifs-dsid-{args.suffix}"
    base = args.os_url.rstrip("/")
    auth = HTTPBasicAuth(args.user, args.password) if args.user else None
    verify = not args.insecure

    print("===================================")
    print("CIFS OpenSearch Validation (dev)")
    print("===================================")
    print(f"OpenSearch: {base}")
    print(f"Index:      {index}\n")

    # 1) count
    try:
        r = request("GET", f"{base}/{index}/_count", auth, verify)
    except requests.RequestException as e:
        print(f"✗ Request failed: {e}", file=sys.stderr)
        return 1
    if r.status_code == 404:
        print(f"✗ Index {index!r} does not exist yet — did the scan run?", file=sys.stderr)
        return 1
    if not r.ok:
        print(f"✗ count failed: {r.status_code} {r.text[:300]}", file=sys.stderr)
        return 1
    count = r.json().get("count", 0)
    print(f"Document count: {count}")

    # 2) sample
    body = {"size": args.show, "query": {"match_all": {}}}
    r = request("GET", f"{base}/{index}/_search", auth, verify, json=body)
    if not r.ok:
        print(f"✗ search failed: {r.status_code} {r.text[:300]}", file=sys.stderr)
        return 1
    hits = r.json().get("hits", {}).get("hits", [])
    print(f"\nSample ({len(hits)} of {count}):")
    for h in hits:
        src = h.get("_source", {})
        url = next(find_key(src, "source_url"), None) or src.get("source_url")
        print(f"  - id={h.get('_id')}")
        print(f"    source_url        = {url}")
        print(f"    file_meta         = {src.get('file_meta') or next(find_key(src, 'file_meta'), None)}")
        print(f"    connector_specific= {src.get('connector_specific') or next(find_key(src, 'connector_specific'), None)}")

    # 3) augment marker
    augmented = 0
    for h in hits:
        if any(v in (True, "true", "True") for v in find_key(h.get("_source", {}), AUGMENT_MARKER)):
            augmented += 1
    # Count across the whole index, not just the sample, via a term-ish search.
    total_augmented = None
    r = request("GET", f"{base}/{index}/_count", auth, verify,
                json={"query": {"term": {f"connector_specific.{AUGMENT_MARKER}": True}}})
    if r.ok:
        total_augmented = r.json().get("count")
    print(f"\nAugment marker '{AUGMENT_MARKER}': {augmented}/{len(hits)} in sample"
          + (f", {total_augmented}/{count} in index (term query)" if total_augmented is not None else ""))

    # verdicts
    ok = True
    if args.expect_count and count < args.expect_count:
        print(f"✗ count {count} < expected {args.expect_count}", file=sys.stderr)
        ok = False
    if args.require_augment:
        # Prefer the index-wide term count; fall back to the sample if the
        # mapping doesn't support the term query (returns 0/None unexpectedly).
        augmented_total = total_augmented if total_augmented else augmented
        denom = count if total_augmented is not None else len(hits)
        if augmented_total < denom or denom == 0:
            print(f"✗ augment incomplete: {augmented_total}/{denom} docs carry "
                  f"'{AUGMENT_MARKER}: true'", file=sys.stderr)
            ok = False

    print("\n✓ Validation passed" if ok else "\n✗ Validation FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
