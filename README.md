# dev-testing-cifs

Scripts for exercising the **CIFS connector** against the **dev** environment (eks-development):
discovery (**scan**) followed by enrichment (**augment**), run sequentially, validating the
results in OpenSearch. Mirror of [`dev-testing`](https://github.com/andres-martinez-nvisionx/dev-testing)
(postgres), adapted to CIFS/SMB.

> Status: phases 1–3 done (scripts + validation). The Samba share for dev now has a manifest
> (`samba-datasource.yaml`, see [Bringing up the CIFS share](#bringing-up-the-cifs-share-and-seeding-it-with-data)),
> and a real E2E run on dev (2026-06-02) indexed 8 docs — see `PLAN.md` T5.2. Remaining open
> items are tracked in `PLAN.md` (T4.4 network policy/KEDA, T5.1/T5.3).

## Flow (scan → augment → validate)

```
kickoff.py cifs           →  scan: discovers the share's files → archiver → OpenSearch
kickoff.py cifs-augment   →  augment: re-resolves every item, lstat + ACL → updates OpenSearch
validate.py               →  counts/inspects pipeline-cifs-dsid-<suffix> in OpenSearch
```

`augment_smoke.py` is optional: it publishes an `ItemBatch` straight to the connector to exercise
augment **without** a prior scan (smoke/stress test of the handler).

## Requirements

- `kubectl` pointed at `eks-development`, with access to the pipeline namespace.
- Python 3.10+.
- The **CIFS connector deployed** in dev. KEDA may have it scaled to 0 — pause autoscaling for the
  duration of the test (same as postgres):
  ```bash
  kubectl -n "$NS" annotate scaledobject <cifs-scaledobject> \
    "autoscaling.keda.sh/paused-replicas=1" --overwrite
  # when done:
  kubectl -n "$NS" annotate scaledobject <cifs-scaledobject> \
    "autoscaling.keda.sh/paused-replicas-" --overwrite
  ```

## Setup

```bash
./setup.sh
source .venv/bin/activate
```

## Port-forwards

The scripts default to `localhost` (port-forward). Start these in another terminal:

```bash
NS=<namespace>
kubectl -n "$NS" port-forward svc/jobengine          18081:8081 &
kubectl -n "$NS" port-forward svc/credential-manager 19090:9090 &
kubectl -n "$NS" port-forward svc/nats                4222:4222 &   # augment_smoke.py only
kubectl -n "$NS" port-forward svc/opensearch          9200:9200 &   # validate.py
```

(`svc` names to be confirmed against the cluster — see `PLAN.md` T4.1.)
In-cluster alternative (running from a pod): pass `--je-url http://jobengine:8081
--credmgr-addr credential-manager:9090`, etc.

## Bringing up the CIFS share and seeding it with data

The connector needs an SMB share to mount. `samba-datasource.yaml` provides one for dev — it is the
CIFS equivalent of `postgres-datasource`, and its credentials match the `kickoff.py` defaults:

| what | value |
|---|---|
| UNC | `//samba/share` |
| user / password | `testuser` / `testpass` |
| workgroup / domain | `WORKGROUP` |
| service | `samba` (ports 445 SMB, 139 NetBIOS) |
| SMB version | `3.0` (kickoff default) |

### Option A — in-cluster (dev)

```bash
NS=default   # the namespace the manifest targets

# 1) Deploy the Samba deployment + service
kubectl apply -f samba-datasource.yaml -n "$NS"

# 2) Wait for the rollout and check the fixtures got seeded
kubectl -n "$NS" rollout status deploy/samba-datasource
kubectl -n "$NS" logs deploy/samba-datasource -c seed-fixtures   # prints the seeded tree

# 3) Confirm the service is resolvable and the share is listed
kubectl -n "$NS" get svc samba
kubectl -n "$NS" run smb-check --image=alpine:3.20 -it --rm -- sh -c \
  'apk add --no-cache samba-client >/dev/null && \
   smbclient -L //samba -U testuser%testpass -m SMB3'

# 4) Browse the seeded tree over SMB
kubectl -n "$NS" run smb-check --image=alpine:3.20 -it --rm -- sh -c \
  'apk add --no-cache samba-client >/dev/null && \
   smbclient //samba/share -U testuser%testpass -m SMB3 -c "recurse; ls"'
```

Removal: `kubectl delete -f samba-datasource.yaml -n "$NS"`.

**How the data gets seeded**: an `initContainer` (`seed-fixtures`, busybox) writes the fixture tree
into the share volume before Samba starts. The volume is an `emptyDir`, so **the data is recreated
from the initContainer on every pod restart** — anything you add at runtime is lost when the pod is
replaced. To change the seeded set permanently, edit the `args` script of the `seed-fixtures`
container in `samba-datasource.yaml` and re-apply.

The seeded tree mirrors `testdata/share/` — 8 files, which is what scan is expected to index:

```
README.md
data/records.csv
docs/notes.md
docs/readme.txt
images/photo.bin              (2 KiB of /dev/urandom)
nested/level1/file_l1.txt
nested/level1/level2/file_l2.txt
reports/quarterly.txt
```

The layout is deliberate, not arbitrary: `docs/` small text files, `reports/` medium files, `data/`
structured data (CSV), `images/` binary fixtures, and `nested/level1/level2/` to exercise recursion
depth and the fork heuristics. Keep it small and deterministic — the goal is exercising scan code
paths (DFS, fork-or-fallback, owner cache, ACL flow), not stress-testing IO.

**Adding extra data at runtime** (transient — gone on pod restart):

```bash
# push a file into the share from a throwaway pod
kubectl -n "$NS" run smb-put --image=alpine:3.20 -it --rm -- sh -c \
  'apk add --no-cache samba-client >/dev/null && echo hello > /tmp/extra.txt && \
   smbclient //samba/share -U testuser%testpass -m SMB3 -c "put /tmp/extra.txt extra.txt"'

# or write straight into the volume through the samba container
kubectl -n "$NS" exec deploy/samba-datasource -c samba -- \
  sh -c 'echo hello > /share/extra.txt; ls -l /share'
```

### Option B — local share (fast loop, no cluster)

Same image and credentials as the manifest, mounting the repo's fixtures directly, so files edited
on the host show up in the share immediately:

```bash
docker run -d --name samba -p 445:445 -p 139:139 \
  -v "$PWD/testdata/share:/share" \
  dperson/samba \
  -u "testuser;testpass" \
  -s "share;/share;yes;no;no;testuser;testuser" \
  -w WORKGROUP -p

# verify
smbclient //localhost/share -U testuser%testpass -m SMB3 -c 'recurse; ls'

# teardown
docker rm -f samba
```

Then point the scripts at it: `export CIFS_FULL_UNC="//localhost/share"` (or
`CIFS_IP_ADDRESS=127.0.0.1`).

### Pointing the scripts at the share

The seeded credential is built from these env vars (defaults in parentheses match
`samba-datasource.yaml`):

```bash
export CIFS_FULL_UNC="//samba/share"   # or the real UNC; derived from CIFS_SERVER_ADDRESS/CIFS_SHARE_NAME
export CIFS_USERNAME=testuser
export CIFS_PASSWORD=testpass
export CIFS_DOMAIN=WORKGROUP
# optional: CIFS_IP_ADDRESS, CIFS_BIND_FOLDER, CIFS_SKIP_ACL (default true),
#           CIFS_SMB_VERSION (default 3.0), CIFS_SERVER_ADDRESS (samba), CIFS_SHARE_NAME (share)
```

Mount prerequisites on the connector pod (confirmed in dev, `PLAN.md` T4.2): `mount.cifs` present
and `SYS_ADMIN` capability granted. The `cifs` kernel module loads on demand — the node's kernel
support is only truly proven by the first successful mount.

## NATS subjects

In dev the subjects do **not** carry the `io.` segment (unlike the local docker-compose setup):

| | local devtools | dev (these scripts' default) |
|---|---|---|
| connector | `je.io.cifs-scanner.v1.in` | `je.cifs-scanner.v1.in` |
| archiver | `je.io.archiver.v1.in` | `je.archiver.v1.in` |
| extractor | `je.io.extractor.v1.in` | `je.extractor.v1.in` |

The connector subject was confirmed against the pod env in dev (`PLAN.md` T4.1):
`je.cifs-scanner.v1.in` · stream `JE_CIFS` · durable `cifs-scanner-v1`. Override any of them with
`--connector-subject` / `--archiver-subject` / `--extractor-subject`, or the env vars
`CIFS_CONNECTOR_SUBJECT` / `CIFS_ARCHIVER_SUBJECT` / `CIFS_EXTRACTOR_SUBJECT`.

## Running

```bash
# 1) Scan
python kickoff.py cifs --suffix 001
python get_credentials.py cifs-access-001          # verify the seeded credential
python validate.py --suffix 001 --expect-count 1   # 8 docs with the fixture tree

# 2) Augment (once scan has indexed items)
python kickoff.py cifs-augment --suffix 001
python validate.py --suffix 001 --require-augment  # requires augment_done:true

# (optional) augment in isolation, without a scan
python augment_smoke.py --paths /README.md /data/records.csv --batches 2 --seed-payload

# 3) Content mode (CONNECTOR_MODE=content) — ALREADY deployed in dev.
#    Run it from an EPHEMERAL pod, NOT from the content pod (it is scaled to zero;
#    if it is at 0 you cannot exec into it). The kickoff wakes it up by itself via KEDA — see
#    "Content mode: how the connector wakes up" below. Use --timeout 300
#    (cold start stacks up: KEDA poll + pod scheduling + mount.cifs).
python content_kickoff.py --path /docs/readme.txt --suffix content-001 --timeout 300
python content_kickoff.py --path /nope.txt --timeout 300            # → FETCH_ERROR_NOT_FOUND
python content_kickoff.py --unc-override //samba/other --timeout 300 # → INVALID_URN (UNC mismatch guard)
# with a real source_url copied from an OpenSearch doc:
python content_kickoff.py --urn 'nx:cifs:v1?access=cifs-access-001&path=%2Fdocs%2Freadme.txt&unc=%2F%2Fsamba%2Fshare' --skip-seed --timeout 300
```

### Content mode: how the connector wakes up (scale-to-zero)

`content_kickoff.py` **does not use JobEngine** — it publishes a `ContentFetchRequest` straight to
the `content.fetch.cifs` subject (captured by the `CONTENT_FETCH` stream) and waits for the reply on
an ephemeral inbox. The content connector runs with **KEDA scale-to-zero**, so it is normally at
**0 replicas**. **No manual scaling is needed**: the kickoff itself wakes it up. The chain:

```
you publish to content.fetch.cifs
  → the CONTENT_FETCH stream persists the message (not lost even with the pod at 0)
  → the `content-fetch-cifs` durable's num_pending goes to 1
  → nats-exporter exposes the metric → Prometheus scrapes it
  → KEDA sees num_pending > 0 and scales the Deployment 0 → 1
  → the pod starts, binds to the (already existing) durable, pulls the message and processes it
```

The **durable** (`content-fetch-cifs`) is a persistent *consumer* that lives on the NATS server,
**not in the pod**: it holds the cursor of which messages are pending/delivered/acked. **NACK**
pre-provisions it via a `kind: Consumer` CR
(`nx-application-helm/.../cifs-connector-be-go-content-crds.yaml`, `pre-install/pre-upgrade` hook),
so it **exists from deploy time** — that is why KEDA can read its `num_pending` even with the pod at
0. Check that it is provisioned:

```bash
kubectl -n "$NS" get consumer | grep content-fetch-cifs   # should show up as Created
```

If the durable does **not** exist (NACK down, or one of the helm flags —
`cifsConnectorBeGoContent.enabled`, `contentFetch.durable`, `contentService.enabled`,
`contentService.stream.enabled` — set to false), KEDA has no `num_pending` to look at and the pod
never wakes up. That is a **config bug to fix**, not a "scale it by hand" situation.

**Where to run the kickoff** — from an **ephemeral pod**, NEVER from the content pod (which is at 0,
and is besides the *consumer*, not the producer):

```bash
kubectl -n "$NS" run kickoff-tmp --image=python:3.12 -it --rm -- bash
# inside:  cd /tmp && curl -L <repo-tar> | tar xz && cd dev-testing-cifs && ./setup.sh
# target the in-cluster services (no port-forwards):
python content_kickoff.py --path /docs/readme.txt --suffix content-001 \
  --nats-url nats://nats:4222 --credmgr-addr credential-manager:9090 --timeout 300
```

Full ephemeral-pod recipe (flags, pulling the repo with `curl`/`kubectl cp`, cleanup):
see [`EPHEMERAL_POD.md`](./EPHEMERAL_POD.md).

**Validating the output**: verification through `/_debug` is opt-in
(`--verify --content-debug-url …`) and only works against the local mock — the real content-service
does not expose it. For a real E2E, the reply gives you `success` + `content_size`; the file travels
over gRPC to content-service and is stored in **S3 keyed by URN** (the connector does **not**
propagate the `sha256` in the NATS reply, so look it up by URN, not by sha).

## Validating in OpenSearch (raw curl)

```bash
curl "localhost:9200/_cat/indices?v" | grep pipeline
curl "localhost:9200/pipeline-cifs-dsid-001/_count?pretty"
curl "localhost:9200/pipeline-cifs-dsid-001/_search?size=5&pretty"
# reset the index
curl -X DELETE "localhost:9200/pipeline-cifs-dsid-001"
```

(OpenSearch in dev may require auth/TLS: `validate.py --user … --password … --insecure`, or add
`-u user:pass -k` to the curl.)

## Files

| file | role |
|---|---|
| `kickoff.py` | scan (`cifs`) and augment (`cifs-augment`): seeds credential + pipeline + job |
| `augment_smoke.py` | augment in isolation: publishes an `ItemBatch` straight to NATS |
| `content_kickoff.py` | content mode: seeds credential + publishes `ContentFetchRequest` (wakes the pod via KEDA) + waits for the reply |
| `get_credentials.py` | reads the seeded CIFS credential (password masked) |
| `validate.py` | validates count + augment marker in OpenSearch |
| `setup.sh` | creates the venv and installs dependencies + `proto_py` |
| `proto_py/` | generated protobuf stubs (credentials, envelope, scan events) |
| `samba-datasource.yaml` | Samba deployment + service for dev, with the fixture tree seeded by an initContainer |
| `testdata/share/` | fixtures to mount into the cluster's Samba |
| `EPHEMERAL_POD.md` | how to run the kickoffs from an in-cluster ephemeral pod (no port-forwards) |
