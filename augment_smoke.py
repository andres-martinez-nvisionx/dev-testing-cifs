#!/usr/bin/env python3
"""
augment_smoke.py — Standalone smoke / stress test for the CIFS connector's
**augment handler** (pb.ItemBatch processing), targeting the **dev** environment.

Dev fork of pipeline-workspace/devtools/augment_smoke.py. Same flow, but the
default subjects drop the `io.` segment (dev convention) and the targets default
to port-forwarded dev services. Override anything via flags / env.

Why this exists alongside `kickoff.py cifs-augment`:
  - `kickoff.py cifs-augment` runs the real pipeline (extract → connector →
    archive) and requires items to already exist from a prior scan.
  - This script publishes pb.ItemBatch directly to the connector's input
    subject with URNs you construct, so augment can be exercised in isolation.

Flow:
  1. Seed credential in CredentialManager (idempotent).
  2. Upsert a 3-step pipeline in JobEngine: extract (AugmentStart placeholder)
     → cifs-augment (ItemBatch) → archive (PublishToNext sink).
  3. Create a Job (mints a job_id the connector resolves via GetJobSettings).
  4. Build a pb.ItemBatch with N items carrying CIFS URNs, JetStream-publish to
     the connector input subject.

Targets default to port-forwards. Set them up first:
  kubectl -n "$NS" port-forward svc/jobengine 18081:8081 &
  kubectl -n "$NS" port-forward svc/credential-manager 19090:9090 &
  kubectl -n "$NS" port-forward svc/nats 4222:4222 &

Usage:
  python augment_smoke.py --paths /README.md /data/records.csv --batches 2
  python augment_smoke.py --count 500 --path-template /stress/{i}.bin
  python augment_smoke.py --skip-seed --skip-pipeline
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from typing import Iterable, List
from urllib.parse import quote

import grpc
import requests
from google.protobuf import any_pb2
from google.protobuf import struct_pb2

from proto_py import cifs_credentials_pb2
from proto_py import credential_requests_pb2
from proto_py import credential_requests_pb2_grpc
from proto_py import envelope_pb2
from proto_py import scan_events_pb2


# --- dev defaults (override via flags / env) --------------------------------
DEFAULT_NATS_URL = "nats://localhost:4222"
DEFAULT_JOBENGINE_URL = "http://localhost:18081"
DEFAULT_CREDMGR_ADDR = "localhost:19090"
# Dev subject convention (no `io.`). CONFIRM via PLAN.md T4.1.
DEFAULT_INPUT_SUBJECT = "je.cifs-scanner.v1.in"
DEFAULT_EXTRACTOR_SUBJECT = "je.extractor.v1.in"
DEFAULT_ARCHIVER_SUBJECT = "je.archiver.v1.in"
DEFAULT_PIPELINE_ID = "cifs-augment-smoke"
DEFAULT_SERVICE_NAME = "augment-smoke"
DEFAULT_DATASOURCE_SUFFIX = "smoke-001"
DEFAULT_FULL_UNC = "//samba/share"
DEFAULT_USERNAME = "testuser"
DEFAULT_PASSWORD = "testpass"
DEFAULT_DOMAIN = "WORKGROUP"


def build_urn(access_id: str, unc: str, path: str) -> str:
    """CIFS content URN matching cifsurn.go: sorted keys (access, path, unc),
    RFC-3986 percent-encoded values."""
    if not path.startswith("/"):
        raise ValueError(f"path must start with '/': {path!r}")
    params = [("access", access_id), ("path", path), ("unc", unc)]
    qs = "&".join(f"{k}={quote(v, safe='')}" for k, v in params)
    return f"nx:cifs:v1?{qs}"


def expand_paths(args: argparse.Namespace) -> List[str]:
    if args.paths:
        return list(args.paths)
    if args.count and args.count > 0:
        tpl = args.path_template or "/smoke/file_{i}.txt"
        if "{i}" not in tpl:
            raise SystemExit(f"--path-template {tpl!r} must contain '{{i}}'")
        return [tpl.format(i=i) for i in range(args.count)]
    return ["/README.md"]


def seed_credential(credmgr_addr: str, datasource_id: str, access_id: str,
                    unc: str, username: str, password: str, domain: str,
                    ip_address: str, bind_folder: str) -> None:
    print(f"[1/3] Seeding credential at {credmgr_addr} for "
          f"datasource_id={datasource_id!r} access_id={access_id!r}")
    channel = grpc.insecure_channel(credmgr_addr)
    try:
        stub = credential_requests_pb2_grpc.CredentialServiceStub(channel)
        creds = cifs_credentials_pb2.CIFSCredentials(
            full_unc=unc, username=username, password=password, domain=domain,
            ip_address=ip_address, smb_version="3.0", bind_folder=bind_folder,
            mount_options={"vers": "3.0", "file_mode": "0755", "dir_mode": "0755"},
        )
        any_creds = any_pb2.Any()
        any_creds.Pack(creds)
        req = credential_requests_pb2.CreateConnectorCredentialRequest(
            datasource_id=datasource_id, datasource_access_id=access_id,
            credentials=any_creds,
        )
        resp = stub.CreateConnectorCredential(req, timeout=10.0)
        if not resp.success:
            msg = (resp.message or "").lower()
            if "exists" in msg or "duplicate" in msg:
                print(f"      already exists — reusing ({resp.message})")
                return
            raise RuntimeError(f"credential seed failed: {resp.message}")
        print(f"      OK — {resp.message}")
    finally:
        channel.close()


def upsert_pipeline(jobengine_url: str, pipeline_id: str, input_subject: str,
                    extractor_subject: str, archiver_subject: str) -> None:
    print(f"[2/3] Upserting pipeline {pipeline_id!r} on {jobengine_url}")
    pipeline = {
        "name": pipeline_id,
        "description": "Standalone CIFS augment smoke test (dev)",
        "steps": [
            {"id": "extract", "service": "extractor",
             "input_subject": extractor_subject,
             "output_subject": "je.ctrl.stepresult.extractor",
             "timeout_seconds": 600, "message_type": "AugmentStart"},
            {"id": "cifs-augment", "service": "cifs-connector",
             "input_subject": input_subject,
             "output_subject": "je.ctrl.stepresult.cifs-scanner",
             "timeout_seconds": 600, "message_type": "ItemBatch"},
            {"id": "archive", "service": "archiver",
             "input_subject": archiver_subject,
             "output_subject": "je.ctrl.stepresult.archiver",
             "timeout_seconds": 60},
        ],
    }
    url = f"{jobengine_url}/api/v1/pipelines/{pipeline_id}"
    resp = requests.put(url, json=pipeline, timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"pipeline upsert failed: {resp.status_code} {resp.text[:300]}")
    print(f"      OK ({resp.status_code})")


def create_job(jobengine_url: str, pipeline_id: str,
               datasource_id: str, access_id: str) -> str:
    print(f"[3/3] Creating job for pipeline {pipeline_id!r}")
    payload = {"pipeline_id": pipeline_id,
               "payload": {"datasource_id": datasource_id, "datasource_access_id": access_id}}
    url = f"{jobengine_url}/api/v1/jobs"
    resp = requests.post(url, json=payload, timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"job create failed: {resp.status_code} {resp.text[:300]}")
    body = resp.json()
    job_id = body.get("id") or body.get("ID")
    if not job_id:
        raise RuntimeError(f"job response missing id field: {body}")
    print(f"      OK — job_id={job_id}")
    return job_id


def build_seed_payload(datasource_id: str, access_id: str) -> any_pb2.Any:
    """Seed Item.Payload with Privacy + a preexisting ConnectorSpecific key so
    the connector's buildAugmentedItem preserve+merge path is exercised."""
    prev_cs = struct_pb2.Struct(fields={
        "discovery_marker": struct_pb2.Value(string_value="from-discovery"),
        "owner": struct_pb2.Value(string_value="OLD_OWNER_FROM_DISCOVERY"),
    })
    prev_cs_any = any_pb2.Any()
    prev_cs_any.Pack(prev_cs)
    privacy = scan_events_pb2.Privacy(
        data_type=scan_events_pb2.Privacy.PII, total=42, run=True,
        matches={"email": scan_events_pb2.Privacy.Match(
            total=10, uniques=5, values=["smoke@example.com"])},
    )
    doc = scan_events_pb2.Document(
        datasource_id=datasource_id, datasource_access_id=access_id,
        connector_type=scan_events_pb2.CIFS, privacy=privacy,
        connector_specific=prev_cs_any,
    )
    doc_any = any_pb2.Any()
    doc_any.Pack(doc)
    return doc_any


def make_items(access_id: str, unc: str, paths: Iterable[str], *,
               datasource_id: str = "", seed_payload: bool = False
               ) -> List[scan_events_pb2.Item]:
    items: List[scan_events_pb2.Item] = []
    seed = build_seed_payload(datasource_id, access_id) if seed_payload else None
    for p in paths:
        urn = build_urn(access_id, unc, p)
        item_id = f"smoke-{uuid.uuid5(uuid.NAMESPACE_URL, urn).hex[:16]}"
        item = scan_events_pb2.Item(item_id=item_id, version=1, source_url=urn)
        if seed is not None:
            item.payload.CopyFrom(seed)
        items.append(item)
    return items


def pack_envelope(job_id: str, items: List[scan_events_pb2.Item], service_name: str) -> bytes:
    batch = scan_events_pb2.ItemBatch(batch_amount=len(items), items=items)
    any_payload = any_pb2.Any()
    any_payload.Pack(batch)
    envelope = envelope_pb2.Envelope(
        id=str(uuid.uuid4()), type="ItemBatch", job_id=job_id,
        process_id=str(uuid.uuid4()), parent_process_id="",
        service_name=service_name, payload=any_payload,
    )
    return envelope.SerializeToString()


async def publish_batches(nats_url: str, subject: str, batches: List[bytes]) -> None:
    import nats
    print(f"Publishing {len(batches)} batch(es) to {subject!r} on {nats_url}")
    nc = await nats.connect(nats_url)
    try:
        js = nc.jetstream()
        for idx, data in enumerate(batches):
            ack = await js.publish(subject, data)
            print(f"      batch {idx + 1}/{len(batches)} — stream={ack.stream} "
                  f"seq={ack.seq} bytes={len(data)}")
    finally:
        await nc.drain()


def split_into_batches(items: List[scan_events_pb2.Item], batches: int) -> List[List[scan_events_pb2.Item]]:
    if batches <= 1 or len(items) <= 1:
        return [items]
    per = max(1, len(items) // batches)
    chunks = [items[i:i + per] for i in range(0, len(items), per)]
    while len(chunks) > batches:
        chunks[-2].extend(chunks[-1])
        chunks.pop()
    return chunks


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Standalone smoke/stress test for the CIFS augment handler (dev).",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)

    parser.add_argument("--nats-url", default=os.getenv("NATS_URL", DEFAULT_NATS_URL))
    parser.add_argument("--jobengine-url", default=os.getenv("JOBENGINE_URL", DEFAULT_JOBENGINE_URL))
    parser.add_argument("--credmgr-addr", default=os.getenv("CREDENTIAL_MANAGER_ADDR", DEFAULT_CREDMGR_ADDR))
    parser.add_argument("--input-subject", default=os.getenv("CIFS_CONNECTOR_SUBJECT") or DEFAULT_INPUT_SUBJECT,
                        help="Subject the cifs-connector consumes (where we publish the ItemBatch)")
    parser.add_argument("--extractor-subject", default=os.getenv("CIFS_EXTRACTOR_SUBJECT") or DEFAULT_EXTRACTOR_SUBJECT)
    parser.add_argument("--archiver-subject", default=os.getenv("CIFS_ARCHIVER_SUBJECT") or DEFAULT_ARCHIVER_SUBJECT)
    parser.add_argument("--pipeline-id", default=DEFAULT_PIPELINE_ID)
    parser.add_argument("--service-name", default=DEFAULT_SERVICE_NAME)

    parser.add_argument("--suffix", default=DEFAULT_DATASOURCE_SUFFIX)
    parser.add_argument("--full-unc", default=os.getenv("CIFS_FULL_UNC") or DEFAULT_FULL_UNC)
    parser.add_argument("--username", default=os.getenv("CIFS_USERNAME") or DEFAULT_USERNAME)
    parser.add_argument("--password", default=os.getenv("CIFS_PASSWORD") or DEFAULT_PASSWORD)
    parser.add_argument("--domain", default=os.getenv("CIFS_DOMAIN") or DEFAULT_DOMAIN)
    parser.add_argument("--ip-address", default=os.getenv("CIFS_IP_ADDRESS") or "")
    parser.add_argument("--bind-folder", default=os.getenv("CIFS_BIND_FOLDER") or "")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--paths", nargs="+", help="Explicit mount-relative paths (e.g. /a.txt /sub/b.txt)")
    group.add_argument("--count", type=int, default=0, help="Generate N paths via --path-template")
    parser.add_argument("--path-template", default="/smoke/file_{i}.txt")
    parser.add_argument("--batches", type=int, default=1)

    parser.add_argument("--skip-seed", action="store_true")
    parser.add_argument("--skip-pipeline", action="store_true")
    parser.add_argument("--skip-publish", action="store_true")
    parser.add_argument("--seed-payload", action="store_true",
                        help="Seed Item.Payload (Privacy + ConnectorSpecific) to exercise preserve+merge")

    args = parser.parse_args()

    datasource_id = f"cifs-dsid-{args.suffix}"
    access_id = f"cifs-access-{args.suffix}"

    print("====================================")
    print("CIFS Augment Smoke Test (dev)")
    print("====================================")
    print(f"  datasource_id     : {datasource_id}")
    print(f"  datasource_access : {access_id}")
    print(f"  full_unc          : {args.full_unc}")
    print(f"  subject           : {args.input_subject}\n")

    try:
        paths = expand_paths(args)
    except SystemExit:
        raise
    except Exception as e:
        print(f"Path resolution failed: {e}", file=sys.stderr)
        return 2
    print(f"  items             : {len(paths)} (first: {paths[0]!r})")
    print(f"  batches           : {args.batches}\n")

    if not args.skip_seed:
        try:
            seed_credential(args.credmgr_addr, datasource_id, access_id, args.full_unc,
                            args.username, args.password, args.domain, args.ip_address, args.bind_folder)
        except (grpc.RpcError, RuntimeError) as e:
            print(f"Credential seeding failed: {e}", file=sys.stderr)
            return 1
    else:
        print("[1/3] Skipping credential seed (--skip-seed)")

    if not args.skip_pipeline:
        try:
            upsert_pipeline(args.jobengine_url, args.pipeline_id, args.input_subject,
                            args.extractor_subject, args.archiver_subject)
        except (requests.RequestException, RuntimeError) as e:
            print(f"Pipeline upsert failed: {e}", file=sys.stderr)
            return 1
    else:
        print("[2/3] Skipping pipeline upsert (--skip-pipeline)")

    try:
        job_id = create_job(args.jobengine_url, args.pipeline_id, datasource_id, access_id)
    except (requests.RequestException, RuntimeError) as e:
        print(f"Job creation failed: {e}", file=sys.stderr)
        return 1

    if args.skip_publish:
        print("\nStopping before publish (--skip-publish). job_id printed above.")
        return 0

    items = make_items(access_id, args.full_unc, paths,
                       datasource_id=datasource_id, seed_payload=args.seed_payload)
    batches = split_into_batches(items, args.batches)
    payloads = [pack_envelope(job_id, b, args.service_name) for b in batches]

    print()
    try:
        asyncio.run(publish_batches(args.nats_url, args.input_subject, payloads))
    except Exception as e:
        print(f"Publish failed: {e}", file=sys.stderr)
        return 1

    print("\n====================================")
    print(f"✓ Published {len(items)} item(s) across {len(payloads)} batch(es)")
    print("====================================")
    print("Next steps:")
    print("  - kubectl -n \"$NS\" logs -f deploy/<cifs-connector> | grep -E 'augment|ItemBatch'")
    print("  - python validate.py --suffix %s --require-augment" % args.suffix)
    return 0


if __name__ == "__main__":
    sys.exit(main())
