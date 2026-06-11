#!/usr/bin/env python3
"""
content_kickoff.py — Manual E2E driver for the CIFS connector's
**content mode** (CONNECTOR_MODE=content), targeting the **dev** environment.

Dev fork of pipeline-workspace/devtools/cifs_content_kickoff.py. Same flow,
but defaults point at port-forwarded dev services and Content-Service
verification is opt-in (--verify): the real content-service may not expose
the mock's /_debug endpoint.

PREREQUISITE: a cifs-connector deployment running CONNECTOR_MODE=content in
dev (helm block `cifsConnectorBeGoContent` — pending follow-up after the
Fase 3 PR merges). Until that exists, requests sit in CONTENT_FETCH
unconsumed and this script times out.

Flow (no JobEngine involvement — no pipeline, no Job):

  1. Seed a CIFSCredentials record in Credential Manager (idempotent).
  2. Build a CIFS content URN (nx:cifs:v1?access=<id>&path=<p>&unc=<u>) for
     a file that exists on the share, or take --urn verbatim (e.g. a
     source_url copied from an OpenSearch doc emitted by the scan).
  3. Publish a ContentFetchRequest to CONTENT_FETCH on content.fetch.cifs,
     reply_subject = ephemeral core-NATS inbox.
  4. Await PROCESSING + terminal COMPLETED reply; print result.

Port-forwards (same as the other scripts in this repo):
  kubectl -n "$NS" port-forward svc/credential-manager 19090:9090 &
  kubectl -n "$NS" port-forward svc/nats                4222:4222 &

Interesting failure modes to exercise:
  --path /nope.txt                  → FETCH_ERROR_NOT_FOUND
  --path /                          → FETCH_ERROR_IS_DIRECTORY
  --unc-override //samba/other      → FETCH_ERROR_INVALID_URN (UNC mismatch
                                      guard: URN's unc snapshot differs from
                                      the credential's current full_unc)

The handler this script exercises lives at
cifs-connector-be-go/internal/contentfetch/handler.go; the error mapping is
documented in cifs-connector-be-go/ARCHITECTURE.md §3.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from urllib.parse import quote

import grpc
from google.protobuf import any_pb2
from google.protobuf.timestamp_pb2 import Timestamp

from proto_py import credential_requests_pb2
from proto_py import credential_requests_pb2_grpc
from proto_py import cifs_credentials_pb2
from proto_py import content_fetch_pb2 as fetchpb


DEFAULT_NATS_URL          = "nats://localhost:4222"
DEFAULT_CREDMGR_ADDR      = "localhost:19090"
DEFAULT_CONTENT_DEBUG_URL = ""  # real content-service: no /_debug; set explicitly if available
DEFAULT_SUBJECT           = "content.fetch.cifs"
DEFAULT_STREAM            = "CONTENT_FETCH"
DEFAULT_DATASOURCE_SUFFIX = "content-001"
DEFAULT_FULL_UNC          = "//samba/share"
DEFAULT_USERNAME          = "testuser"
DEFAULT_PASSWORD          = "testpass"
DEFAULT_DOMAIN            = "WORKGROUP"
DEFAULT_PATH              = "/docs/readme.txt"  # exists in testdata/share fixtures


def build_urn(access_id: str, unc: str, path: str) -> str:
    """
    Build a CIFS content URN matching the canonical form produced by
    cifs-connector-be-go/internal/cifsurn/cifsurn.go (which delegates to
    pipeline-lib-go/urn.Build).

    Key ordering is sorted ASCII (access, path, unc) and values are RFC-3986
    percent-encoded — `quote(..., safe="")` matches the upper-case %XX hex
    output of the Go encoder.
    """
    if not path.startswith("/"):
        raise ValueError(f"path must start with '/': {path!r}")
    params = [
        ("access", access_id),
        ("path",   path),
        ("unc",    unc),
    ]
    qs = "&".join(f"{k}={quote(v, safe='')}" for k, v in params)
    return f"nx:cifs:v1?{qs}"


def seed_credential(credmgr_addr: str, datasource_id: str, access_id: str,
                    unc: str, username: str, password: str, domain: str,
                    ip_address: str, bind_folder: str) -> None:
    """
    Create a CIFS credential in Credential Manager. Idempotent: if the
    credential already exists, log + continue.
    """
    print(f"[1/2] Seeding credential at {credmgr_addr} for "
          f"datasource_id={datasource_id!r} access_id={access_id!r}")
    channel = grpc.insecure_channel(credmgr_addr)
    try:
        stub = credential_requests_pb2_grpc.CredentialServiceStub(channel)

        creds = cifs_credentials_pb2.CIFSCredentials(
            full_unc=unc,
            username=username,
            password=password,
            domain=domain,
            ip_address=ip_address,
            smb_version="3.0",
            bind_folder=bind_folder,
            mount_options={
                "vers": "3.0",
                "file_mode": "0755",
                "dir_mode": "0755",
            },
        )
        any_creds = any_pb2.Any()
        any_creds.Pack(creds)

        req = credential_requests_pb2.CreateConnectorCredentialRequest(
            datasource_id=datasource_id,
            datasource_access_id=access_id,
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


async def ensure_content_stream(js, stream_name: str, subject_pattern: str) -> None:
    """Verify (or create) the CONTENT_FETCH JetStream stream.

    In dev the stream should be pre-provisioned by the platform; the
    create fallback keeps the script usable against a bare NATS. Idempotent.
    """
    from nats.js.api import StreamConfig
    from nats.js.errors import NotFoundError

    try:
        info = await js.stream_info(stream_name)
        covered = any(
            subject_pattern == s or (s.endswith(".>") and subject_pattern.startswith(s[:-2]))
            for s in info.config.subjects or []
        )
        if not covered:
            print(f"      WARN: stream {stream_name!r} exists but doesn't match "
                  f"subject {subject_pattern!r} (has {info.config.subjects})")
        else:
            print(f"      stream {stream_name!r} already exists — reusing")
        return
    except NotFoundError:
        pass

    await js.add_stream(config=StreamConfig(
        name=stream_name,
        subjects=["content.fetch.>"],  # matches all per-connector content subjects
    ))
    print(f"      created JetStream stream {stream_name!r} → content.fetch.>")


async def publish_and_wait(
    nats_url: str,
    subject: str,
    stream: str,
    urn: str,
    timeout: float,
) -> fetchpb.ContentFetchResponse:
    """Publish ContentFetchRequest to JetStream, await terminal reply."""
    import nats

    print(f"[2/2] Publishing ContentFetchRequest to {subject!r} via {nats_url}")
    nc = await nats.connect(nats_url)
    try:
        js = nc.jetstream()
        await ensure_content_stream(js, stream, subject)

        inbox = nc.new_inbox()
        fut: asyncio.Future = asyncio.Future()

        async def on_msg(m):
            resp = fetchpb.ContentFetchResponse()
            resp.ParseFromString(m.data)
            status_name = fetchpb.FetchResponseStatus.Name(resp.status)
            if resp.status == fetchpb.FETCH_STATUS_PROCESSING:
                print(f"      ← PROCESSING (request_id={resp.request_id})")
                return
            if not fut.done():
                print(f"      ← {status_name} (request_id={resp.request_id})")
                fut.set_result(resp)

        sub = await nc.subscribe(inbox, cb=on_msg)
        await nc.flush()

        req = fetchpb.ContentFetchRequest(
            request_id=f"cifs-kickoff-{uuid.uuid4().hex[:12]}",
            urn=urn,
            reply_subject=inbox,
        )
        ts = Timestamp()
        ts.GetCurrentTime()
        req.timestamp.CopyFrom(ts)

        print(f"      request_id={req.request_id}")
        print(f"      urn={urn}")

        ack = await js.publish(subject, req.SerializeToString())
        print(f"      JetStream ACK: stream={ack.stream} seq={ack.seq}")

        try:
            resp = await asyncio.wait_for(fut, timeout=timeout)
            return resp
        finally:
            await sub.unsubscribe()
    finally:
        await nc.drain()


def verify_in_content_service(debug_url: str, urn: str) -> None:
    """GET the uploaded payload from a Content Service /_debug endpoint.

    Only works against the mock content service (or any deployment exposing
    the same debug route). Opt-in via --verify + --content-debug-url.
    """
    try:
        import requests
    except ImportError:
        print("      (requests not installed — skipping Content Service verify. "
              "pip install requests to enable.)")
        return

    encoded = quote(urn, safe="")
    url = f"{debug_url.rstrip('/')}/_debug/urn/{encoded}"
    try:
        r = requests.get(url, timeout=5)
    except requests.RequestException as e:
        print(f"      Content Service GET failed: {e}")
        return

    if r.status_code == 404:
        print("      Content Service has no payload for this URN (404). "
              "Either the upload didn't reach it, or the debug endpoint "
              "doesn't recognize this URN.")
        return
    if r.status_code != 200:
        print(f"      Content Service GET unexpected status {r.status_code}: {r.text[:200]}")
        return

    size = r.headers.get("X-Content-Size", "?")
    sha256 = r.headers.get("X-Content-Sha256", "?")
    print(f"      Content Service GET OK — size={size} bytes, sha256={sha256}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Content-mode kickoff for the CIFS connector (dev environment).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--urn", default=None,
                        help="Full content URN (e.g. a source_url copied from an OpenSearch doc); "
                             "overrides --path/--suffix/--full-unc URN building")
    parser.add_argument("--path", default=DEFAULT_PATH,
                        help=f"Share-relative file path for the URN (default {DEFAULT_PATH})")
    parser.add_argument("--suffix", default=DEFAULT_DATASOURCE_SUFFIX,
                        help="Datasource suffix → access_id=cifs-access-<suffix>")
    parser.add_argument("--unc-override", default=None,
                        help="Use a different unc in the URN than the seeded credential "
                             "(exercises the UNC-mismatch guard → INVALID_URN)")

    parser.add_argument("--nats-url",     default=os.getenv("NATS_URL", DEFAULT_NATS_URL))
    parser.add_argument("--credmgr-addr", default=os.getenv("CREDENTIAL_MANAGER_ADDR", DEFAULT_CREDMGR_ADDR))
    parser.add_argument("--stream",       default=DEFAULT_STREAM, help="JetStream stream name (created if missing)")
    parser.add_argument("--subject",      default=os.getenv("CONTENT_FETCH_SUBJECT", DEFAULT_SUBJECT))
    parser.add_argument("--timeout",      type=float, default=120.0,
                        help="Reply wait timeout in seconds (dev cold-start can be slow: "
                             "KEDA scale-up + mount.cifs)")

    parser.add_argument("--verify", action="store_true",
                        help="Verify the upload via a /_debug HTTP endpoint (mock-style only)")
    parser.add_argument("--content-debug-url",
                        default=os.getenv("CONTENT_DEBUG_URL", DEFAULT_CONTENT_DEBUG_URL),
                        help="Base URL of the Content Service debug HTTP endpoint (requires --verify)")

    parser.add_argument("--skip-seed",   action="store_true", help="Skip credential seeding (already seeded)")
    parser.add_argument("--full-unc",    default=os.getenv("CIFS_FULL_UNC") or DEFAULT_FULL_UNC)
    parser.add_argument("--username",    default=os.getenv("CIFS_USERNAME") or DEFAULT_USERNAME)
    parser.add_argument("--password",    default=os.getenv("CIFS_PASSWORD") or DEFAULT_PASSWORD)
    parser.add_argument("--domain",      default=os.getenv("CIFS_DOMAIN") or DEFAULT_DOMAIN)
    parser.add_argument("--ip-address",  default=os.getenv("CIFS_IP_ADDRESS") or "")
    parser.add_argument("--bind-folder", default=os.getenv("CIFS_BIND_FOLDER") or "")

    args = parser.parse_args()

    datasource_id = f"cifs-dsid-{args.suffix}"
    access_id     = f"cifs-access-{args.suffix}"

    if args.urn:
        urn = args.urn
    else:
        urn_unc = args.unc_override or args.full_unc
        try:
            urn = build_urn(access_id, urn_unc, args.path)
        except ValueError as e:
            print(f"Invalid path: {e}", file=sys.stderr)
            return 2

    print("====================================")
    print("CIFS Content-Mode Kickoff (dev)")
    print("====================================")
    print(f"  access_id : {access_id}")
    print(f"  urn       : {urn}")

    if not args.skip_seed:
        try:
            seed_credential(
                credmgr_addr=args.credmgr_addr,
                datasource_id=datasource_id,
                access_id=access_id,
                unc=args.full_unc,
                username=args.username,
                password=args.password,
                domain=args.domain,
                ip_address=args.ip_address,
                bind_folder=args.bind_folder,
            )
        except (grpc.RpcError, RuntimeError) as e:
            print(f"Credential seeding failed: {e}", file=sys.stderr)
            return 1
    else:
        print("[1/2] Skipping credential seed (--skip-seed)")

    try:
        resp = asyncio.run(
            publish_and_wait(
                nats_url=args.nats_url,
                subject=args.subject,
                stream=args.stream,
                urn=urn,
                timeout=args.timeout,
            )
        )
    except asyncio.TimeoutError:
        print(f"Timed out after {args.timeout}s waiting for reply. Check that the "
              f"cifs content deployment exists in dev (CONNECTOR_MODE=content), "
              f"is scaled > 0, and consumes {args.subject!r}.",
              file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Publish/wait failed: {e}", file=sys.stderr)
        return 1

    print("------------------------------------")
    print("Reply details")
    print("------------------------------------")
    print(f"  success      : {resp.success}")
    print(f"  status       : {fetchpb.FetchResponseStatus.Name(resp.status)}")
    print(f"  duration_ms  : {resp.duration_ms}")
    if resp.success:
        print(f"  urn          : {resp.urn}")
        print(f"  content_size : {resp.content_size} bytes")
    else:
        err = resp.error
        print(f"  error.code   : {fetchpb.FetchErrorCode.Name(err.code)}")
        print(f"  error.msg    : {err.message}")
        print(f"  retryable    : {err.retryable}")

    if resp.success and args.verify:
        if not args.content_debug_url:
            print("--verify set but no --content-debug-url provided — skipping.")
        else:
            print("------------------------------------")
            print(f"Verifying upload at {args.content_debug_url}")
            print("------------------------------------")
            verify_in_content_service(args.content_debug_url, resp.urn or urn)

    return 0 if resp.success else 1


if __name__ == "__main__":
    sys.exit(main())
