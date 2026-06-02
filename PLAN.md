# PLAN.md — dev-testing-cifs

Objetivo: repo clonable dentro del entorno **dev** (eks-development) que reproduce contra
dev lo que hoy hace `pipeline-workspace/devtools/` en local para CIFS: sembrar credenciales,
correr **scan** (`kickoff.py cifs`) y luego **augment** (`kickoff.py cifs-augment`) de forma
secuencial, y validar resultados en OpenSearch.

Espejo del repo `dev-testing` (postgres). Ubicación: `~/Documents/Proyectos/Nvisionx/dev-testing-cifs`.
Remote sugerido: `git@github.com:andres-martinez-nvisionx/dev-testing-cifs.git`.

Diferencia clave vs postgres: en dev **no existe un samba** (la única referencia en todo el
workspace es el `docker-compose.yml` local). Postgres tenía `postgres-datasource` ya desplegado;
para CIFS hay que proveer el share en el cluster. → Decisión de samba DIFERIDA hasta tener cluster
(ver Fase 4). Todo lo demás se arma ahora.

Fuentes a copiar/adaptar:
- `pipeline-workspace/devtools/kickoff.py` (lógica CIFS scan/augment, subjects, credencial proto)
- `pipeline-workspace/devtools/augment_smoke.py` (augment aislado)
- `pipeline-workspace/devtools/get_credentials.py` (verificación de credenciales)
- `pipeline-workspace/devtools/proto_py/` (protos ya generados: cifs_credentials, credential_requests, envelope, archiver, jobengine)
- `dev-testing/{setup.sh,README.md,proto_py/pyproject.toml}` (patrón de bootstrap dev)
- `cifs-connector-be-go/testdata/share/` (fixtures de prueba)

⚠️ Discrepancia confirmada de NATS subjects:
- local devtools: `je.io.cifs-scanner.v1.in` / `je.io.archiver.v1.in` / out `je.ctrl.stepresult.cifs-scanner`
- dev postgres:   `je.postgresql-scanner.v1.in` (SIN `io.`) / `je.archiver.v1.in`
→ En dev el subject de CIFS probablemente sea `je.cifs-scanner.v1.in`. NO asumir: confirmar leyendo
el env del pod del connector en dev (Fase 4, T4.1). Los subjects van parametrizados por flag/env.

---

## Fase 1 — Esqueleto del repo

- [x] T1.1 — Crear `dev-testing-cifs/` con `git init`, `.gitignore` (venv/, __pycache__/, *.pyc, .pytest_cache/).
  Criterio: `git status` limpio salvo archivos del plan.
- [x] T1.2 — Copiar `proto_py/` desde `pipeline-workspace/devtools/proto_py/` (incluye
  `cifs_credentials_pb2*`, `credential_requests_pb2*`, `envelope_pb2*`, `archiver_pb2*`,
  `jobengine_pb2*`, `cifs_fork_pb2*` + `__init__.py` + `pyproject.toml`).
  Criterio: `pip install -e ./proto_py` importa `from proto_py import cifs_credentials_pb2` sin error.
- [x] T1.3 — `setup.sh` adaptado de `dev-testing/setup.sh` (detect OS, venv, `pip install grpcio
  protobuf requests nats-py opensearch-py`, `pip install -e ./proto_py`).
  Criterio: `bash setup.sh` deja venv funcional con imports OK.
- [x] T1.4 — Copiar fixtures a `testdata/share/` desde `cifs-connector-be-go/testdata/share/`
  (README.md, docs/, data/records.csv, images/photo.bin, nested/level1/level2/, reports/).
  Criterio: árbol idéntico al de testdata (≈9 archivos).

## Fase 2 — Scripts de kickoff (scan + augment)

- [x] T2.1 — `kickoff.py` adaptado de `devtools/kickoff.py`, recortado a CIFS (`cifs`, `cifs-augment`).
  Defaults a dev: credmgr `localhost:19090` (port-forward) o `credential-manager:9090` (in-cluster),
  JobEngine `localhost:18081` (port-forward). Subjects vía flag `--connector-subject` /
  `--archiver-subject` con default a confirmar en T4.1 (probable `je.cifs-scanner.v1.in` / `je.archiver.v1.in`).
  Credencial CIFS por env: `CIFS_FULL_UNC`, `CIFS_USERNAME`, `CIFS_PASSWORD`, `CIFS_DOMAIN`,
  `CIFS_IP_ADDRESS`, `CIFS_BIND_FOLDER`, `CIFS_SKIP_ACL`, `CIFS_SMB_VERSION`.
  IDs: `cifs-dsid-{suffix}` / `cifs-access-{suffix}`.
  Criterio: `python kickoff.py cifs --help` y `cifs-augment --help` listan flags; dry-run construye
  payloads correctos (pipeline 2-step scan→archive; augment 3-step extract→cifs-augment→archive).
- [x] T2.2 — `augment_smoke.py` copiado/adaptado de devtools (publica ItemBatch directo a NATS,
  default NATS dev `nats://localhost:4222` por port-forward). URN `nx:cifs:v1?access=&path=&unc=`.
  Criterio: `--help` OK; construye envelope válido sin requerir scan previo.
- [x] T2.3 — `get_credentials.py` copiado de devtools (verificar credencial CIFS sembrada vía gRPC).
  Criterio: imprime campos CIFS (server, share, user, password enmascarado, domain, smb_version).

## Fase 3 — Validación OpenSearch

- [x] T3.1 — `validate.py`: query a `pipeline-cifs-dsid-{suffix}` en OpenSearch dev (port-forward
  `localhost:9200` o endpoint dev). Cuenta docs, muestra muestra de `source_url`/`file_meta`/
  `connector_specific`, y un check de `augment` (campos de permisos/owner presentes tras augment).
  Flags: `--suffix`, `--os-url`, `--expect-count`, `--require-augment`.
  Criterio: tras scan reporta N docs; con `--require-augment` falla si faltan campos de augment.
- [x] T3.2 — Snippets curl equivalentes en README (count, _search, _mapping, DELETE índice para reset).

## Fase 4 — Samba en dev (DIFERIDO — requiere cluster)

> No ejecutar hasta tener acceso a eks-development (VPN). Cuando haya cluster:

- [x] T4.1 — Env del pod confirmado (ns `default`). VALORES REALES:
  - subject scan/augment: `je.cifs-scanner.v1.in` (sin `io.`) · stream `JE_CIFS` · durable `cifs-scanner-v1`
  - JobEngine HTTP: `http://applications-job-engine-v2:8081` · gRPC `applications-job-engine-v2:9090`
  - Credential Manager gRPC: `applications-credentialmanager:9090`
  - NATS: `nats://nats:4222`
  - imagen viva: `alpine-development-2026.06.01-57` (post-augment → fix B funcionó, rolió solo)
  - OpenSearch: NO en env del connector (lo usa el archiver); hay `applications-opensearchbroker:8080` → confirmar para validate.py
  Defaults de kickoff.py ya alineados (subject + port-forward a svc reales).
- [x] T4.2 — Montaje CIFS OK: `mount.cifs` v7.4 presente + `CapEff=0x1ffffffffff` (todas las caps,
  SYS_ADMIN incluido). El pod puede montar. (`cifs` fs carga on-demand; falta validar que el kernel
  del nodo EKS tenga el módulo — se confirma al primer mount.)
- [x] T4.3 — `samba-datasource.yaml` escrito (Deployment+Service `dperson/samba`, initContainer
  busybox que siembra el árbol de fixtures, service `samba` :445). Matchea defaults de kickoff
  (`//samba/share`, testuser/testpass, WORKGROUP). PENDIENTE: `kubectl apply` + verificar rollout.
  Criterio: el connector resuelve el UNC y monta el share; `//samba/share` accesible desde el pod.
- [ ] T4.4 — Network policy connector→samba (445/139) y KEDA (pausar réplicas como en postgres:
  `autoscaling.keda.sh/paused-replicas=1`) si aplica.
  Criterio: tráfico permitido; connector no se autoescala a 0 durante la prueba.

## Fase 5 — Documentación y prueba E2E

- [ ] T5.1 — `README.md` con el flujo completo dev (port-forwards necesarios, orden:
  desplegar/verificar samba → `setup.sh` → seed+scan `kickoff.py cifs` → `validate.py` →
  augment `kickoff.py cifs-augment` → `validate.py --require-augment`), troubleshooting y cleanup.
  Criterio: alguien sin contexto corre la prueba siguiendo solo el README.
- [ ] T5.2 — Corrida E2E real en dev: scan indexa los ~9 items en `pipeline-cifs-dsid-*`; augment
  los actualiza con permisos/ACL/owner. Validado con `validate.py`.
  Criterio: ambos scripts verdes y OpenSearch refleja scan luego augment.
- [ ] T5.3 — `git remote add` + primer commit (commit lo hace el usuario manualmente).

---

## Notas / decisiones tomadas
- Estructura: repo nuevo `dev-testing-cifs` (no subcarpeta) — espejo de `dev-testing`.
- Samba en dev: DIFERIDO a Fase 4 (decidir con cluster accesible).
- Cluster eks-development inaccesible al momento de planear (API server timeout — probable VPN).

## Riesgos
- Subject naming dev ≠ local (`io.` segment) → T4.1 bloquea T5.2 si no se confirma.
- `mount.cifs` requiere SYS_ADMIN en el pod del connector (T4.2) → posible bloqueante de infra.
- OpenSearch dev puede requerir auth/TLS (no como el local sin auth) → ajustar `validate.py`.
