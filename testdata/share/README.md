# Test fixture: CIFS share tree

This directory is mounted as a Samba share by the workspace's `samba` service
(see `pipeline-workspace/docker-compose.yml`). The connector mounts it via
`mount.cifs` during e2e tests driven by `devtools/kickoff.py cifs`.

Keep the tree small and deterministic — the goal is exercising scan code paths
(DFS, fork-or-fallback, owner cache, ACL flow), not stress-testing IO.

Layout
- `docs/` — small text files
- `reports/` — medium files
- `data/` — CSV / structured data
- `images/` — binary fixtures
- `nested/level1/level2/` — exercises recursion depth and fork heuristics
