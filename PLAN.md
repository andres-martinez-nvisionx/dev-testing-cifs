# PLAN.md — dev-testing-cifs

Goal: a repo that can be cloned inside the **dev** environment (eks-development) and reproduces
against dev what `pipeline-workspace/devtools/` does locally for CIFS: seed credentials, run **scan**
(`kickoff.py cifs`) and then **augment** (`kickoff.py cifs-augment`) sequentially, and validate the
results in OpenSearch.

Mirror of the `dev-testing` repo (postgres). Location: `~/Documents/Proyectos/Nvisionx/dev-testing-cifs`.
Suggested remote: `git@github.com:andres-martinez-nvisionx/dev-testing-cifs.git`.

Key difference vs postgres: dev has **no samba** (the only reference in the whole workspace is the
local `docker-compose.yml`). Postgres already had `postgres-datasource` deployed; for CIFS the share
has to be provisioned in the cluster. → The samba decision was DEFERRED until cluster access was
available (see Phase 4). Everything else was built up front.

Sources to copy/adapt:
- `pipeline-workspace/devtools/kickoff.py` (CIFS scan/augment logic, subjects, credential proto)
- `pipeline-workspace/devtools/augment_smoke.py` (augment in isolation)
- `pipeline-workspace/devtools/get_credentials.py` (credential verification)
- `pipeline-workspace/devtools/proto_py/` (already-generated protos: cifs_credentials, credential_requests, envelope, archiver, jobengine)
- `dev-testing/{setup.sh,README.md,proto_py/pyproject.toml}` (dev bootstrap pattern)
- `cifs-connector-be-go/testdata/share/` (test fixtures)

⚠️ Confirmed NATS subject discrepancy:
- local devtools: `je.io.cifs-scanner.v1.in` / `je.io.archiver.v1.in` / out `je.ctrl.stepresult.cifs-scanner`
- dev postgres:   `je.postgresql-scanner.v1.in` (WITHOUT `io.`) / `je.archiver.v1.in`
→ In dev the CIFS subject is probably `je.cifs-scanner.v1.in`. Do NOT assume: confirm by reading the
connector pod's env in dev (Phase 4, T4.1). The subjects are parameterized by flag/env.

---

## Phase 1 — Repo skeleton

- [x] T1.1 — Create `dev-testing-cifs/` with `git init` and a `.gitignore` (venv/, __pycache__/, *.pyc, .pytest_cache/).
  Criterion: `git status` clean except for the plan files.
- [x] T1.2 — Copy `proto_py/` from `pipeline-workspace/devtools/proto_py/` (includes
  `cifs_credentials_pb2*`, `credential_requests_pb2*`, `envelope_pb2*`, `archiver_pb2*`,
  `jobengine_pb2*`, `cifs_fork_pb2*` + `__init__.py` + `pyproject.toml`).
  Criterion: `pip install -e ./proto_py` makes `from proto_py import cifs_credentials_pb2` import cleanly.
- [x] T1.3 — `setup.sh` adapted from `dev-testing/setup.sh` (OS detection, venv, `pip install grpcio
  protobuf requests nats-py opensearch-py`, `pip install -e ./proto_py`).
  Criterion: `bash setup.sh` leaves a working venv with imports OK.
- [x] T1.4 — Copy fixtures into `testdata/share/` from `cifs-connector-be-go/testdata/share/`
  (README.md, docs/, data/records.csv, images/photo.bin, nested/level1/level2/, reports/).
  Criterion: tree identical to testdata (≈9 files).

## Phase 2 — Kickoff scripts (scan + augment)

- [x] T2.1 — `kickoff.py` adapted from `devtools/kickoff.py`, trimmed down to CIFS (`cifs`, `cifs-augment`).
  Defaults for dev: credmgr `localhost:19090` (port-forward) or `credential-manager:9090` (in-cluster),
  JobEngine `localhost:18081` (port-forward). Subjects via `--connector-subject` /
  `--archiver-subject` flags, with defaults to be confirmed in T4.1 (likely `je.cifs-scanner.v1.in` /
  `je.archiver.v1.in`). CIFS credential from env: `CIFS_FULL_UNC`, `CIFS_USERNAME`, `CIFS_PASSWORD`,
  `CIFS_DOMAIN`, `CIFS_IP_ADDRESS`, `CIFS_BIND_FOLDER`, `CIFS_SKIP_ACL`, `CIFS_SMB_VERSION`.
  IDs: `cifs-dsid-{suffix}` / `cifs-access-{suffix}`.
  Criterion: `python kickoff.py cifs --help` and `cifs-augment --help` list the flags; a dry run builds
  the right payloads (2-step scan→archive pipeline; 3-step augment extract→cifs-augment→archive).
- [x] T2.2 — `augment_smoke.py` copied/adapted from devtools (publishes an ItemBatch straight to NATS,
  dev NATS default `nats://localhost:4222` over port-forward). URN `nx:cifs:v1?access=&path=&unc=`.
  Criterion: `--help` OK; builds a valid envelope without needing a prior scan.
- [x] T2.3 — `get_credentials.py` copied from devtools (verify the seeded CIFS credential over gRPC).
  Criterion: prints the CIFS fields (server, share, user, masked password, domain, smb_version).

## Phase 3 — OpenSearch validation

- [x] T3.1 — `validate.py`: queries `pipeline-cifs-dsid-{suffix}` in dev OpenSearch (port-forward
  `localhost:9200` or the dev endpoint). Counts docs, shows a sample of `source_url`/`file_meta`/
  `connector_specific`, and an `augment` check (permission/owner fields present after augment).
  Flags: `--suffix`, `--os-url`, `--expect-count`, `--require-augment`.
  Criterion: after a scan it reports N docs; with `--require-augment` it fails if augment fields are missing.
- [x] T3.2 — Equivalent curl snippets in the README (count, _search, _mapping, index DELETE for reset).

## Phase 4 — Samba in dev (DEFERRED — requires cluster)

> Not to be executed until eks-development is reachable (VPN). Once the cluster is available:

- [x] T4.1 — Pod env confirmed (ns `default`). REAL VALUES:
  - scan/augment subject: `je.cifs-scanner.v1.in` (no `io.`) · stream `JE_CIFS` · durable `cifs-scanner-v1`
  - JobEngine HTTP: `http://applications-job-engine-v2:8081` · gRPC `applications-job-engine-v2:9090`
  - Credential Manager gRPC: `applications-credentialmanager:9090`
  - NATS: `nats://nats:4222`
  - live image: `alpine-development-2026.06.01-57` (post-augment → fix B worked, it rolled out on its own)
  - OpenSearch: NOT in the connector's env (the archiver uses it); there is
    `applications-opensearchbroker:8080` → confirm for validate.py
  kickoff.py defaults already aligned (subject + port-forward to the real svcs).
- [x] T4.2 — CIFS mount OK: `mount.cifs` v7.4 present + `CapEff=0x1ffffffffff` (all caps, SYS_ADMIN
  included). The pod can mount. (The `cifs` fs loads on demand; still to be validated that the EKS
  node's kernel has the module — confirmed on the first mount.)
- [x] T4.3 — `samba-datasource.yaml` written (Deployment+Service `dperson/samba`, busybox
  initContainer that seeds the fixture tree, `samba` service on :445). Matches the kickoff defaults
  (`//samba/share`, testuser/testpass, WORKGROUP). PENDING: `kubectl apply` + verify the rollout.
  Criterion: the connector resolves the UNC and mounts the share; `//samba/share` reachable from the pod.
- [ ] T4.4 — Network policy connector→samba (445/139) and KEDA (pause replicas as with postgres:
  `autoscaling.keda.sh/paused-replicas=1`) if applicable.
  Criterion: traffic allowed; the connector does not autoscale to 0 during the test.

## Phase 5 — Documentation and E2E test

- [ ] T5.1 — `README.md` with the full dev flow (required port-forwards, order: deploy/verify samba →
  `setup.sh` → seed+scan `kickoff.py cifs` → `validate.py` → augment `kickoff.py cifs-augment` →
  `validate.py --require-augment`), troubleshooting and cleanup.
  Criterion: someone without context can run the test following only the README.
- [x] T5.2 — Real E2E run in dev (2026-06-02): scan indexed 8 v1 docs into
  `cifs-access-001-nxidx-000001` (POSIX owner `root`). Augment validated via `augment_smoke.py`
  (ItemBatch straight to `je.cifs-scanner.v1.in`) → 8 `version:2` docs with SMB owner/group
  (SIDs `S-1-22-1-100`/`S-1-22-2-101`). Augment handler OK in dev.
  NOTE: the "real" augment via the extractor (`kickoff.py cifs-augment`) does NOT work in dev — the
  extractor processes but does not forward the ItemBatch to `je.cifs-scanner.v1.in` (JE_CIFS never
  receives it). Box augments INLINE (does not use the extractor), postgres does scan+content → nobody
  exercises that handoff in dev. This is a platform question (does cifs augment in prod go through the
  extractor or inline?), NOT a connector bug.
- [ ] T5.3 — `git remote add` + first commit (the commit is done manually by the user).

---

## Notes / decisions taken
- Structure: new `dev-testing-cifs` repo (not a subfolder) — mirror of `dev-testing`.
- Samba in dev: DEFERRED to Phase 4 (to be decided with the cluster reachable).
- The eks-development cluster was unreachable at planning time (API server timeout — likely VPN).

## Risks
- dev subject naming ≠ local (`io.` segment) → T4.1 blocks T5.2 if not confirmed.
- `mount.cifs` requires SYS_ADMIN on the connector pod (T4.2) → possible infra blocker.
- dev OpenSearch may require auth/TLS (unlike the local one without auth) → adjust `validate.py`.
