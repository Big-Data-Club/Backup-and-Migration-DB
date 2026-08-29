# Changelog

## 2.0.0 — 2026-08-29

- Added the interactive `./start_migrate` entry point.
- Added automatic discovery and migration of every user database.
- Added PostgreSQL major-version detection and parallel directory-format
  dump/restore.
- Added per-database table-count verification and TSV run reports.
- Added `search_path` repair and Neon pooler backend-session recycling.
- Added local CoreApplication `.env` and ConfigMap updates.
- Added transactional production K3s cutover with writer quiescing, saved
  replicas/configuration, automatic rollback, Redis synchronization, and
  rollout verification.
- Added bounded dump retention.
- Added threshold-based K3s maintenance to Core deployments; unused images are
  pruned only under disk pressure instead of after every deployment.
