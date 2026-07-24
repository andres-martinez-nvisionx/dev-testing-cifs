# Running the kickoff from an ephemeral pod

Reference for running this repo's scripts (chiefly `content_kickoff.py`) from **inside the
cluster**, without port-forwards and **without** exec'ing into the connector pod.

## Why an ephemeral pod

- The ClusterIP `Service`s (`nats`, `credential-manager`, `content-service`, `jobengine`) are only
  routable **inside** the cluster. From outside you need `port-forward` (which in turn requires
  kubectl against the API). An ephemeral pod **is born on the cluster network** → it sees all those
  services over internal DNS, no tunnels.
- In **content mode** the connector runs with KEDA **scale-to-zero**. The kickoff is the *producer*
  (it publishes the `ContentFetchRequest`); the content pod is the *consumer*. Running the kickoff
  inside the content pod is circular: at 0 replicas you cannot exec into it, and it is precisely the
  message that wakes it up. Hence a **separate** pod.
- `--rm` makes the pod **delete itself when you exit the shell** → no leftovers.

## Launching the pod

```bash
NS=<namespace>     # the pipeline namespace in dev

kubectl -n "$NS" run kickoff-tmp \
  --image=python:3.12 \
  --restart=Never \
  -it --rm \
  -- bash
```

| flag | what it does |
|---|---|
| `run kickoff-tmp` | creates a new Pod (IP on the cluster network → access to the ClusterIP services) |
| `--image=python:3.12` | base image with Python (your "temporary machine") |
| `--restart=Never` | bare Pod, not a Deployment — otherwise K8s would recreate it on exit |
| `-it` | interactive shell (TTY) |
| `--rm` | **deletes the pod on exit** (`exit` / Ctrl-D) |
| `-- bash` | command to run inside |

## Inside the pod: pull the repo + setup

The filesystem is **ephemeral** (everything is lost when the pod dies), so this is repeated every
session. Two ways to get the repo in:

**Option A — `curl` (requires internet egress from the pod):**

```bash
cd /tmp
curl -L <repo-tar-url> | tar xz   # release tarball, or GitHub with a token if private
cd dev-testing-cifs
```

**Option B — `kubectl cp` from CloudShell (no internet in the pod, ideal for a private repo):**

```bash
# FROM CloudShell (not inside the pod), with the pod already running:
kubectl -n "$NS" cp ./dev-testing-cifs kickoff-tmp:/tmp/dev-testing-cifs
# then, inside the pod:
cd /tmp/dev-testing-cifs
```

Setup (the same either way):

```bash
./setup.sh
source .venv/bin/activate
```

## Running the kickoff (targeting in-cluster services)

No port-forwards: internal DNS names are used. **Confirm the `svc` names against the cluster** (they
may differ):

```bash
python content_kickoff.py \
  --path /docs/readme.txt --suffix content-001 \
  --nats-url nats://nats:4222 \
  --credmgr-addr credential-manager:9090 \
  --timeout 300
```

- `--timeout 300`: the content cold start stacks up latencies (KEDA poll ~30s + pod scheduling +
  `mount.cifs`). 120s can fall short.
- To target the **real dev share** (rather than the local `//samba/share` fixture), export the CIFS
  env vars first (`CIFS_FULL_UNC`, `CIFS_USERNAME`, …) or pass `--full-unc` / `--path` with a file
  that actually exists on that share.

For `kickoff.py` (scan/augment) the idea is the same, plus JobEngine:

```bash
python kickoff.py cifs --suffix 001 \
  --je-url http://jobengine:8081 \
  --credmgr-addr credential-manager:9090
```

## Cleanup

`--rm` deletes the pod on exit. If the connection dropped and it is left hanging:

```bash
kubectl -n "$NS" get pod kickoff-tmp
kubectl -n "$NS" delete pod kickoff-tmp
```

## Note: "ephemeral" has two meanings

- **This flow** (`kubectl run --rm`): a new **standalone** Pod. This is what we use here.
- **`kubectl debug` / ephemeral containers**: injecting a container **into an existing pod** to debug
  it. Not our case (we do not want to get into the content pod, we want a separate one).
