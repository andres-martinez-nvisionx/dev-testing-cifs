# dev-testing-cifs

Scripts para probar el **CIFS connector** contra el entorno **dev** (eks-development):
discovery (**scan**) y luego enriquecimiento (**augment**) de forma secuencial, validando
resultados en OpenSearch. Espejo de [`dev-testing`](https://github.com/andres-martinez-nvisionx/dev-testing)
(postgres), adaptado a CIFS/SMB.

> Estado: Fases 1–3 listas (scripts + validación). **Falta proveer el Samba en el cluster
> (Fase 4)** — ver `PLAN.md`. Hasta entonces, `kickoff.py` siembra credenciales pero el
> connector no tendrá un share que montar.

## Flujo (scan → augment → validate)

```
kickoff.py cifs           →  scan: descubre archivos del share → archiver → OpenSearch
kickoff.py cifs-augment   →  augment: re-resuelve cada item, lstat + ACL → actualiza OpenSearch
validate.py               →  cuenta/inspecciona pipeline-cifs-dsid-<suffix> en OpenSearch
```

`augment_smoke.py` es opcional: publica `ItemBatch` directo al connector para probar el augment
**sin** scan previo (smoke/stress del handler).

## Requisitos

- `kubectl` apuntando a `eks-development` y acceso al namespace de la pipeline.
- Python 3.10+.
- El **CIFS connector desplegado** en dev. KEDA puede tenerlo escalado a 0 — pausar el autoscaling
  durante la prueba (igual que postgres):
  ```bash
  kubectl -n "$NS" annotate scaledobject <cifs-scaledobject> \
    "autoscaling.keda.sh/paused-replicas=1" --overwrite
  # al terminar:
  kubectl -n "$NS" annotate scaledobject <cifs-scaledobject> \
    "autoscaling.keda.sh/paused-replicas-" --overwrite
  ```

## Setup

```bash
./setup.sh
source .venv/bin/activate
```

## Port-forwards

Los scripts apuntan por default a `localhost` (port-forward). Levantar en otra terminal:

```bash
NS=<namespace>
kubectl -n "$NS" port-forward svc/jobengine          18081:8081 &
kubectl -n "$NS" port-forward svc/credential-manager 19090:9090 &
kubectl -n "$NS" port-forward svc/nats                4222:4222 &   # solo augment_smoke.py
kubectl -n "$NS" port-forward svc/opensearch          9200:9200 &   # validate.py
```

(Nombres de `svc` a confirmar contra el cluster — ver `PLAN.md` T4.1.)
Alternativa in-cluster (correr desde un pod): pasar `--je-url http://jobengine:8081
--credmgr-addr credential-manager:9090`, etc.

## Configuración del share (Fase 4 — pendiente)

No hay Samba en dev todavía. Una vez provisto (manifest nuevo o share existente), exportar:

```bash
export CIFS_FULL_UNC="//samba/share"   # o el UNC real
export CIFS_USERNAME=testuser
export CIFS_PASSWORD=testpass
export CIFS_DOMAIN=WORKGROUP
# opcional: CIFS_IP_ADDRESS, CIFS_BIND_FOLDER, CIFS_SKIP_ACL (default true), CIFS_SMB_VERSION
```

Los fixtures de prueba están en `testdata/share/` (los mismos que usa el setup local en
docker-compose) para montar dentro del Samba del cluster.

## NATS subjects (¡confirmar!)

En dev los subjects **no** llevan el segmento `io.` (a diferencia del docker-compose local):

| | local devtools | dev (default de estos scripts) |
|---|---|---|
| connector | `je.io.cifs-scanner.v1.in` | `je.cifs-scanner.v1.in` |
| archiver | `je.io.archiver.v1.in` | `je.archiver.v1.in` |
| extractor | `je.io.extractor.v1.in` | `je.extractor.v1.in` |

Estos defaults son una **inferencia** (del patrón postgres) y hay que confirmarlos leyendo el env
del pod del connector (`PLAN.md` T4.1). Override con `--connector-subject` / `--archiver-subject`
/ `--extractor-subject` o las env vars `CIFS_CONNECTOR_SUBJECT` / `CIFS_ARCHIVER_SUBJECT` /
`CIFS_EXTRACTOR_SUBJECT`.

## Ejecución

```bash
# 1) Scan
python kickoff.py cifs --suffix 001
python get_credentials.py cifs-access-001          # verificar credencial sembrada
python validate.py --suffix 001 --expect-count 1   # ~9 docs con los fixtures

# 2) Augment (después de que el scan haya indexado items)
python kickoff.py cifs-augment --suffix 001
python validate.py --suffix 001 --require-augment  # exige augment_done:true

# (opcional) augment aislado sin scan
python augment_smoke.py --paths /README.md /data/records.csv --batches 2 --seed-payload
```

## Validación en OpenSearch (curl directo)

```bash
curl "localhost:9200/_cat/indices?v" | grep pipeline
curl "localhost:9200/pipeline-cifs-dsid-001/_count?pretty"
curl "localhost:9200/pipeline-cifs-dsid-001/_search?size=5&pretty"
# reset del índice
curl -X DELETE "localhost:9200/pipeline-cifs-dsid-001"
```

(OpenSearch en dev puede requerir auth/TLS: `validate.py --user … --password … --insecure`,
o agregar `-u user:pass -k` al curl.)

## Archivos

| archivo | rol |
|---|---|
| `kickoff.py` | scan (`cifs`) y augment (`cifs-augment`): siembra credencial + pipeline + job |
| `augment_smoke.py` | augment aislado: publica `ItemBatch` directo a NATS |
| `get_credentials.py` | lee la credencial CIFS sembrada (password enmascarado) |
| `validate.py` | valida count + augment marker en OpenSearch |
| `setup.sh` | crea venv e instala dependencias + `proto_py` |
| `proto_py/` | stubs protobuf generados (credenciales, envelope, scan events) |
| `testdata/share/` | fixtures para montar en el Samba del cluster |
