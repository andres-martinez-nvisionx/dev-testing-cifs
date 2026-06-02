#!/usr/bin/env python3
"""
get_credentials.py — Fetch and print a CIFS credential from CredentialManager.

Verifies that `kickoff.py` seeded the credential correctly (password masked).

Usage:
  python get_credentials.py cifs-access-001
  python get_credentials.py --by-datasource-id cifs-dsid-001
  python get_credentials.py --credmgr-addr localhost:19090 cifs-access-001

Default addr is the port-forward (localhost:19090); override with --credmgr-addr
or CREDENTIAL_MANAGER_ADDR (e.g. credential-manager:9090 when run in-cluster).
"""

import argparse
import os
import sys

import grpc

from proto_py import credential_requests_pb2
from proto_py import credential_requests_pb2_grpc
from proto_py import cifs_credentials_pb2


DEFAULT_CREDMGR_ADDR = "localhost:19090"


def get_credentials(addr: str, lookup_value: str, by_datasource_id: bool) -> int:
    print("===================================")
    print("Get Credentials")
    print("===================================")
    label = "Datasource ID" if by_datasource_id else "Datasource Access ID"
    print(f"{label}: {lookup_value}")
    print(f"CredentialManager: {addr}\n")

    channel = grpc.insecure_channel(addr)
    try:
        stub = credential_requests_pb2_grpc.CredentialServiceStub(channel)
        if by_datasource_id:
            req = credential_requests_pb2.GetConnectorCredentialRequest(datasource_id=lookup_value)
        else:
            req = credential_requests_pb2.GetConnectorCredentialRequest(datasource_access_id=lookup_value)
        resp = stub.GetConnectorCredential(req, timeout=10.0)

        print(f"Datasource ID: {resp.datasource_id}")
        print(f"Credentials type URL: {resp.credentials.type_url}\n")

        if "CIFSCredentials" in resp.credentials.type_url:
            c = cifs_credentials_pb2.CIFSCredentials()
            resp.credentials.Unpack(c)
            print("CIFS Credentials:")
            print(f"  Full UNC:       {c.full_unc}")
            print(f"  Server Address: {c.server_address}")
            print(f"  Share Name:     {c.share_name}")
            print(f"  IP Address:     {c.ip_address}")
            print(f"  Username:       {c.username}")
            print(f"  Password:       {c.password[:2] + '****' if c.password else '(empty)'}")
            print(f"  Domain:         {c.domain}")
            print(f"  SMB Version:    {c.smb_version}")
            print(f"  Bind Folder:    {c.bind_folder}")
            print(f"  Mount Options:  {dict(c.mount_options)}")
        else:
            print(f"Raw credentials (non-CIFS type): {resp.credentials.value}")

        print("\n✓ Credentials retrieved successfully")
        return 0
    except grpc.RpcError as e:
        print(f"✗ gRPC error: {e.code()}: {e.details()}", file=sys.stderr)
        return 1
    finally:
        channel.close()


def main() -> None:
    p = argparse.ArgumentParser(description="Fetch a CIFS credential from CredentialManager")
    p.add_argument("lookup_value", help="datasource_access_id (default) or datasource_id")
    p.add_argument("--by-datasource-id", action="store_true",
                   help="Look up by datasource_id instead of datasource_access_id")
    p.add_argument("--credmgr-addr", default=os.getenv("CREDENTIAL_MANAGER_ADDR") or DEFAULT_CREDMGR_ADDR,
                   help=f"CredentialManager gRPC addr (default: {DEFAULT_CREDMGR_ADDR})")
    args = p.parse_args()
    sys.exit(get_credentials(args.credmgr_addr, args.lookup_value, args.by_datasource_id))


if __name__ == "__main__":
    main()
