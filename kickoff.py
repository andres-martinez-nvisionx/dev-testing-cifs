#!/usr/bin/env python3
"""
kickoff.py — CIFS scan + augment kickoff against the **dev** environment.

Trimmed CIFS-only fork of pipeline-workspace/devtools/kickoff.py, with defaults
pointed at dev (port-forwarded JobEngine/CredentialManager) instead of the local
docker-compose stack.

Two modes, run SEQUENTIALLY:
  python kickoff.py cifs           # 1) scan/discovery → archive
  python kickoff.py cifs-augment   # 2) augment the items the scan produced

How it talks to dev:
  - CredentialManager: gRPC (seed CIFS credentials)
  - JobEngine:         HTTP (register pipeline + create job)
The connector then consumes ScanStart / ItemBatch off its NATS input subject,
mounts the share, and emits Items the archiver indexes into OpenSearch under
`pipeline-cifs-dsid-<suffix>`.

  Local workstation (recommended): port-forward then use defaults
    kubectl -n default port-forward svc/applications-job-engine-v2 18081:8081 &
    kubectl -n default port-forward svc/applications-credentialmanager 19090:9090 &
    python kickoff.py cifs

  In-cluster (run from a pod with python):
    python kickoff.py cifs \
      --je-url http://applications-job-engine-v2:8081 \
      --credmgr-addr applications-credentialmanager:9090

NATS subjects in dev (CONFIRMED via connector env, PLAN.md T4.1):
  connector input : je.cifs-scanner.v1.in   (no `io.` — confirmed)
  stream / durable: JE_CIFS / cifs-scanner-v1
  archiver/extractor subjects below follow the same no-`io.` convention
  (postgres pattern); override with --archiver-subject / --extractor-subject
  or CIFS_ARCHIVER_SUBJECT / CIFS_EXTRACTOR_SUBJECT if they differ.
"""

import argparse
import json
import os
import sys
from typing import Any, Dict

import grpc
import requests
from google.protobuf import any_pb2

from proto_py import credential_requests_pb2
from proto_py import credential_requests_pb2_grpc
from proto_py import cifs_credentials_pb2


# --- dev defaults (override via flags / env) --------------------------------
DEFAULT_JOBENGINE_URL = "http://localhost:18081"   # port-forward svc/jobengine 18081:8081
DEFAULT_CREDMGR_ADDR = "localhost:19090"           # port-forward svc/credential-manager 19090:9090

# Dev subject convention (no `io.` segment). CONFIRMED via connector env (T4.1):
# JOBENGINE_INPUT_SUBJECT=je.cifs-scanner.v1.in / JOBENGINE_STREAM=JE_CIFS.
DEFAULT_CONNECTOR_SUBJECT = "je.cifs-scanner.v1.in"
DEFAULT_ARCHIVER_SUBJECT = "je.archiver.v1.in"
DEFAULT_EXTRACTOR_SUBJECT = "je.extractor.v1.in"


def cifs_env() -> Dict[str, str]:
    """Resolve CIFS connection params from env, with placeholders for dev.

    There is no default Samba host in dev (unlike local docker-compose), so
    CIFS_FULL_UNC / CIFS_SERVER_ADDRESS should be set once the share is
    provisioned (PLAN.md Fase 4). The placeholder mirrors the in-cluster
    service name we expect to deploy.
    """
    server = os.getenv("CIFS_SERVER_ADDRESS") or "samba"
    share_name = os.getenv("CIFS_SHARE_NAME") or "share"
    return {
        "server_address": server,
        "share_name": share_name,
        "full_unc": os.getenv("CIFS_FULL_UNC") or f"//{server}/{share_name}",
        "username": os.getenv("CIFS_USERNAME") or "testuser",
        "password": os.getenv("CIFS_PASSWORD") or "testpass",
        "domain": os.getenv("CIFS_DOMAIN") or "WORKGROUP",
        "ip_address": os.getenv("CIFS_IP_ADDRESS") or "",
        "bind_folder": os.getenv("CIFS_BIND_FOLDER") or "",
        "smb_version": os.getenv("CIFS_SMB_VERSION") or "3.0",
        "skip_acl": os.getenv("CIFS_SKIP_ACL", "true"),
    }


class CifsKickoff:
    connector_type = "cifs"
    service_name = "cifs-connector"

    def __init__(self, suffix: str, jobengine_url: str, credmgr_addr: str,
                 connector_subject: str, archiver_subject: str,
                 extractor_subject: str):
        self.datasource_id = f"cifs-dsid-{suffix}"
        self.datasource_access_id = f"cifs-access-{suffix}"
        self.jobengine_url = jobengine_url
        self.credmgr_addr = credmgr_addr
        self.connector_subject = connector_subject
        self.archiver_subject = archiver_subject
        self.extractor_subject = extractor_subject
        self.env = cifs_env()

    # -- step 1: credentials --------------------------------------------------
    def credentials(self) -> Any:
        e = self.env
        return cifs_credentials_pb2.CIFSCredentials(
            full_unc=e["full_unc"],
            ip_address=e["ip_address"],
            username=e["username"],
            password=e["password"],
            domain=e["domain"],
            smb_version=e["smb_version"],
            mount_options={"vers": e["smb_version"], "file_mode": "0755", "dir_mode": "0755"},
            bind_folder=e["bind_folder"],
        )

    def create_credentials(self) -> bool:
        print("Step 1: Create CIFS Credentials")
        print("------------------------")
        print(f"CredentialManager: {self.credmgr_addr}")
        print(f"full_unc:          {self.env['full_unc']}")
        channel = grpc.insecure_channel(self.credmgr_addr)
        try:
            stub = credential_requests_pb2_grpc.CredentialServiceStub(channel)
            any_creds = any_pb2.Any()
            any_creds.Pack(self.credentials())
            req = credential_requests_pb2.CreateConnectorCredentialRequest(
                datasource_id=self.datasource_id,
                datasource_access_id=self.datasource_access_id,
                credentials=any_creds,
            )
            resp = stub.CreateConnectorCredential(req, timeout=10.0)
            if resp.success:
                print(f"✓ Credentials created: {resp.message}\n")
                return True
            msg = (resp.message or "").lower()
            if "exists" in msg or "duplicate" in msg:
                print(f"✓ Credentials already exist — reusing ({resp.message})\n")
                return True
            print(f"✗ Credential creation failed: {resp.message}", file=sys.stderr)
            return False
        except grpc.RpcError as e:
            print(f"✗ gRPC error: {e.code()}: {e.details()}", file=sys.stderr)
            return False
        finally:
            channel.close()

    # -- step 2: pipeline -----------------------------------------------------
    def pipeline_id(self) -> str:
        return "cifs-scan-archive"

    def pipeline_definition(self) -> Dict[str, Any]:
        return {
            "name": self.pipeline_id(),
            "description": "CIFS Scan to Archive Pipeline (dev)",
            "steps": [
                {
                    "id": "cifs-scan",
                    "service": self.service_name,
                    "input_subject": self.connector_subject,
                    "output_subject": "je.ctrl.stepresult.cifs-scanner",
                    "timeout_seconds": 300,
                    "message_type": "ScanStart",
                },
                {
                    "id": "archive",
                    "service": "archiver",
                    "input_subject": self.archiver_subject,
                    "output_subject": "je.ctrl.stepresult.archiver",
                    "timeout_seconds": 60,
                },
            ],
        }

    def create_pipeline(self) -> bool:
        print("Step 2: Create Pipeline")
        print("------------------------")
        pipeline = self.pipeline_definition()
        url = f"{self.jobengine_url}/api/v1/pipelines/{self.pipeline_id()}"
        print(f"PUT {url}")
        print(json.dumps(pipeline, indent=2))
        try:
            resp = requests.put(url, json=pipeline, timeout=30)
            print(f"→ {resp.status_code} {resp.text}\n")
            if 200 <= resp.status_code < 300:
                print("✓ Pipeline created\n")
                return True
            print("✗ Pipeline creation failed", file=sys.stderr)
            return False
        except requests.RequestException as e:
            print(f"✗ Pipeline request failed: {e}", file=sys.stderr)
            return False

    # -- step 3: job ----------------------------------------------------------
    def job_payload(self) -> Dict[str, Any]:
        e = self.env
        return {
            "metadata": {
                "ip_address": e["ip_address"],
                "cifs_share_path": e["share_name"],
                "bind_folder": e["bind_folder"],
                "username": e["username"],
                "domain": e["domain"],
                # Connector reads skip_acl != "false" — only literal "false"
                # turns ACL collection ON. Default "true" matches prod / fast E2E.
                "skip_acl": e["skip_acl"],
            }
        }

    def create_job(self) -> bool:
        print("Step 3: Create Job")
        print("------------------")
        job = {
            "pipeline_id": self.pipeline_id(),
            "payload": {
                "datasource_id": self.datasource_id,
                "datasource_access_id": self.datasource_access_id,
                **self.job_payload(),
            },
        }
        url = f"{self.jobengine_url}/api/v1/jobs"
        print(f"POST {url}")
        print(json.dumps(job, indent=2))
        try:
            resp = requests.post(url, json=job, timeout=30)
            print(f"→ {resp.status_code} {resp.text}\n")
            if 200 <= resp.status_code < 300:
                print("✓ Job created")
                return True
            print("✗ Job creation failed", file=sys.stderr)
            return False
        except requests.RequestException as e:
            print(f"✗ Job request failed: {e}", file=sys.stderr)
            return False

    def run(self) -> int:
        print("===================================")
        print(f"CIFS {self.mode_label()} — Job Kickoff (dev)")
        print("===================================\n")
        if not self.create_credentials():
            return 1
        if not self.create_pipeline():
            return 1
        if not self.create_job():
            return 1
        print("\n✓ Kickoff completed. Validate with:  python validate.py "
              f"--suffix {self.datasource_id.rsplit('-', 1)[-1]}\n")
        return 0

    def mode_label(self) -> str:
        return "Scan"


class CifsAugmentKickoff(CifsKickoff):
    """Augment pipeline: extract (AugmentStart placeholder) → cifs-augment
    (ItemBatch) → archive. Requires items already in OpenSearch from a prior
    `kickoff.py cifs` run. The connector demuxes ItemBatch from ScanStart on
    the shared input subject."""

    def mode_label(self) -> str:
        return "Augment"

    def pipeline_id(self) -> str:
        return "cifs-augment-extract"

    def pipeline_definition(self) -> Dict[str, Any]:
        return {
            "name": self.pipeline_id(),
            "description": "CIFS Augment Pipeline (dev)",
            "steps": [
                {
                    "id": "extract",
                    "service": "extractor",
                    "input_subject": self.extractor_subject,
                    "output_subject": "je.ctrl.stepresult.extractor",
                    "timeout_seconds": 600,
                    "message_type": "AugmentStart",
                },
                {
                    "id": "cifs-augment",
                    "service": self.service_name,
                    "input_subject": self.connector_subject,
                    "output_subject": "je.ctrl.stepresult.cifs-scanner",
                    "timeout_seconds": 600,
                    "message_type": "ItemBatch",
                },
                {
                    "id": "archive",
                    "service": "archiver",
                    "input_subject": self.archiver_subject,
                    "output_subject": "je.ctrl.stepresult.archiver",
                    "timeout_seconds": 60,
                },
            ],
        }


def main() -> None:
    modes = {"cifs": CifsKickoff, "cifs-augment": CifsAugmentKickoff}
    p = argparse.ArgumentParser(description="CIFS scan/augment kickoff (dev)")
    p.add_argument("mode", nargs="?", default="cifs", choices=modes.keys(),
                   help="cifs = scan/discovery, cifs-augment = augment (default: cifs)")
    p.add_argument("--je-url", default=os.getenv("JOBENGINE_URL") or DEFAULT_JOBENGINE_URL,
                   help=f"JobEngine HTTP URL (default: {DEFAULT_JOBENGINE_URL})")
    p.add_argument("--credmgr-addr", default=os.getenv("CREDENTIAL_MANAGER_ADDR") or DEFAULT_CREDMGR_ADDR,
                   help=f"CredentialManager gRPC addr (default: {DEFAULT_CREDMGR_ADDR})")
    p.add_argument("--connector-subject",
                   default=os.getenv("CIFS_CONNECTOR_SUBJECT") or DEFAULT_CONNECTOR_SUBJECT,
                   help=f"Connector NATS input subject (default: {DEFAULT_CONNECTOR_SUBJECT})")
    p.add_argument("--archiver-subject",
                   default=os.getenv("CIFS_ARCHIVER_SUBJECT") or DEFAULT_ARCHIVER_SUBJECT,
                   help=f"Archiver NATS input subject (default: {DEFAULT_ARCHIVER_SUBJECT})")
    p.add_argument("--extractor-subject",
                   default=os.getenv("CIFS_EXTRACTOR_SUBJECT") or DEFAULT_EXTRACTOR_SUBJECT,
                   help=f"Extractor NATS input subject (default: {DEFAULT_EXTRACTOR_SUBJECT})")
    p.add_argument("--suffix", default="001", help="Datasource id suffix (default: 001)")
    args = p.parse_args()

    kickoff = modes[args.mode](
        suffix=args.suffix,
        jobengine_url=args.je_url,
        credmgr_addr=args.credmgr_addr,
        connector_subject=args.connector_subject,
        archiver_subject=args.archiver_subject,
        extractor_subject=args.extractor_subject,
    )
    sys.exit(kickoff.run())


if __name__ == "__main__":
    main()
