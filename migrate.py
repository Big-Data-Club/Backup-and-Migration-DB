#!/usr/bin/env python3
"""
Neon PostgreSQL Migration Tool
Migrate multiple databases from one Neon host to another host or local PostgreSQL.

Usage:
    python migrate_neon.py --config config.yaml
    python migrate_neon.py --source "postgresql://..." --target "postgresql://..." --all-dbs
"""

import os
import sys
import json
import time
import logging
import argparse
import subprocess
import threading
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional
from pathlib import Path

import psycopg2
import psycopg2.extras
import yaml

# ─── Logging Setup ────────────────────────────────────────────────────────────

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / f"migration_{datetime.now():%Y%m%d_%H%M%S}.log"),
    ],
)
logger = logging.getLogger("neon_migrator")

# ─── State Tracking ───────────────────────────────────────────────────────────

STATE_FILE = Path("migration_state.json")

@dataclass
class DBMigrationStatus:
    db_name: str
    status: str = "pending"        # pending | running | success | failed | skipped
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    error: Optional[str] = None
    size_bytes: int = 0
    tables_count: int = 0
    rows_migrated: int = 0
    dump_path: Optional[str] = None
    duration_seconds: float = 0.0
    retries: int = 0

@dataclass
class MigrationState:
    run_id: str
    source_host: str
    target_host: str
    target_type: str               # neon | local | rds | supabase
    started_at: str
    finished_at: Optional[str] = None
    overall_status: str = "running"
    databases: dict = field(default_factory=dict)

    def save(self, path: Path = STATE_FILE):
        with open(path, "w") as f:
            json.dump(asdict(self), f, indent=2, default=str)
        logger.debug(f"State saved -> {path}")

    @classmethod
    def load(cls, path: Path = STATE_FILE) -> Optional["MigrationState"]:
        if not path.exists():
            return None
        with open(path) as f:
            data = json.load(f)
        state = cls(**{k: v for k, v in data.items() if k != "databases"})
        state.databases = {
            db: DBMigrationStatus(**info) for db, info in data.get("databases", {}).items()
        }
        return state

# ─── Config ───────────────────────────────────────────────────────────────────

@dataclass
class MigrationConfig:
    source_url: str
    targets: list[dict]            # [{type, url, label}]
    exclude_dbs: list[str] = field(default_factory=lambda: ["template0", "template1", "postgres"])
    include_dbs: list[str] = field(default_factory=list)   # empty = all
    dump_dir: str = "dumps"
    parallel_workers: int = 2
    max_retries: int = 3
    retry_delay_seconds: int = 30
    pg_dump_extra_args: list[str] = field(default_factory=list)
    verify_after_migrate: bool = True
    clean_dumps_after: bool = False
    notify_webhook: Optional[str] = None

    @classmethod
    def from_yaml(cls, path: str) -> "MigrationConfig":
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(**data)

    @classmethod
    def from_args(cls, args) -> "MigrationConfig":
        targets = [{"type": args.target_type, "url": args.target, "label": args.target_type}]
        return cls(source_url=args.source, targets=targets)

# ─── Database Discovery ───────────────────────────────────────────────────────

def list_databases(conn_url: str, exclude: list[str]) -> list[dict]:
    """List all user databases on source host."""
    conn = psycopg2.connect(conn_url)
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            d.datname AS name,
            pg_database_size(d.datname) AS size_bytes,
            pg_encoding_to_char(d.encoding) AS encoding,
            r.rolname AS owner
        FROM pg_database d
        JOIN pg_roles r ON d.datdba = r.oid
        WHERE d.datistemplate = false
          AND d.datname NOT IN %s
        ORDER BY d.datname;
    """, (tuple(exclude),))

    databases = [dict(row) for row in cur.fetchall()]
    cur.close()
    conn.close()
    logger.info(f"Found {len(databases)} databases: {[d['name'] for d in databases]}")
    return databases


def count_tables_and_rows(conn_url: str, db_name: str) -> tuple[int, int]:
    """Count tables and approximate total rows for a database."""
    try:
        db_url = swap_database(conn_url, db_name)
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) AS tables,
                COALESCE(SUM(c.reltuples::bigint), 0) AS approx_rows
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast');
        """)
        tables, rows = cur.fetchone()
        cur.close()
        conn.close()
        return int(tables), int(rows)
    except Exception as e:
        logger.warning(f"Could not count tables for {db_name}: {e}")
        return 0, 0


def swap_database(conn_url: str, db_name: str) -> str:
    """Replace database name in connection URL."""
    from urllib.parse import urlparse, urlunparse
    parsed = urlparse(conn_url)
    new_path = f"/{db_name}"
    return urlunparse(parsed._replace(path=new_path))

# ─── Migration Engine ─────────────────────────────────────────────────────────

class NeonMigrator:
    def __init__(self, config: MigrationConfig):
        self.config = config
        self.dump_dir = Path(config.dump_dir)
        self.dump_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def run_migration(self, state: MigrationState) -> bool:
        """Orchestrate full migration for all databases."""
        databases = list_databases(self.config.source_url, self.config.exclude_dbs)

        if self.config.include_dbs:
            databases = [d for d in databases if d["name"] in self.config.include_dbs]

        # Initialize state for each DB
        for db in databases:
            tables, rows = count_tables_and_rows(self.config.source_url, db["name"])
            state.databases[db["name"]] = DBMigrationStatus(
                db_name=db["name"],
                size_bytes=db["size_bytes"],
                tables_count=tables,
                rows_migrated=rows,
            )

        state.save()
        logger.info(f"Starting migration of {len(databases)} databases -> {len(self.config.targets)} target(s)")

        # Use thread pool for parallel migrations
        from concurrent.futures import ThreadPoolExecutor, as_completed
        all_success = True

        with ThreadPoolExecutor(max_workers=self.config.parallel_workers) as executor:
            futures = {
                executor.submit(self._migrate_db_with_retry, db["name"], state): db["name"]
                for db in databases
            }
            for future in as_completed(futures):
                db_name = futures[future]
                success = future.result()
                if not success:
                    all_success = False

        state.overall_status = "success" if all_success else "partial_failure"
        state.finished_at = datetime.utcnow().isoformat()
        state.save()

        self._print_summary(state)
        return all_success

    def _migrate_db_with_retry(self, db_name: str, state: MigrationState) -> bool:
        """Migrate one database with retry logic."""
        db_status = state.databases[db_name]
        max_retries = self.config.max_retries

        for attempt in range(1, max_retries + 1):
            try:
                db_status.status = "running"
                db_status.started_at = datetime.utcnow().isoformat()
                db_status.retries = attempt - 1
                state.save()

                logger.info(f"[{db_name}] Attempt {attempt}/{max_retries} - dumping...")
                t0 = time.time()

                dump_path = self._dump_database(db_name)
                db_status.dump_path = str(dump_path)

                for target in self.config.targets:
                    logger.info(f"[{db_name}] Restoring -> {target['label']} ({target['type']})")
                    self._restore_database(dump_path, db_name, target)

                if self.config.verify_after_migrate:
                    for target in self.config.targets:
                        self._verify_migration(db_name, target)

                if self.config.clean_dumps_after:
                    dump_path.unlink(missing_ok=True)

                db_status.status = "success"
                db_status.finished_at = datetime.utcnow().isoformat()
                db_status.duration_seconds = round(time.time() - t0, 2)
                db_status.error = None
                state.save()
                logger.info(f"[{db_name}] ✅ Done in {db_status.duration_seconds}s")
                return True

            except Exception as e:
                logger.error(f"[{db_name}] Attempt {attempt} failed: {e}")
                db_status.error = str(e)
                if attempt < max_retries:
                    logger.info(f"[{db_name}] Retrying in {self.config.retry_delay_seconds}s...")
                    time.sleep(self.config.retry_delay_seconds)
                else:
                    db_status.status = "failed"
                    db_status.finished_at = datetime.utcnow().isoformat()
                    state.save()
                    return False

    def _dump_database(self, db_name: str) -> Path:
        """Run pg_dump for a single database."""
        dump_path = self.dump_dir / f"{db_name}_{datetime.now():%Y%m%d_%H%M%S}.dump"
        source_db_url = swap_database(self.config.source_url, db_name)

        cmd = [
            "pg_dump",
            "--format=custom",
            "--no-owner",
            "--no-acl",
            "--verbose",
            f"--file={dump_path}",
            *self.config.pg_dump_extra_args,
            source_db_url,
        ]

        logger.debug(f"[{db_name}] pg_dump command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)

        if result.returncode != 0:
            raise RuntimeError(f"pg_dump failed:\n{result.stderr}")

        size_mb = dump_path.stat().st_size / 1024 / 1024
        logger.info(f"[{db_name}] Dump created: {dump_path} ({size_mb:.1f} MB)")
        return dump_path

    def _restore_database(self, dump_path: Path, db_name: str, target: dict):
        """Restore dump to target using pg_restore."""
        target_url = target["url"]
        target_type = target.get("type", "postgres")

        # Create the database on target if not exists
        self._create_database_if_not_exists(target_url, db_name, target_type)

        target_db_url = swap_database(target_url, db_name)

        cmd = [
            "pg_restore",
            "--no-owner",
            "--no-acl",
            "--verbose",
            "--clean",
            "--if-exists",
            f"--dbname={target_db_url}",
            str(dump_path),
        ]

        logger.debug(f"[{db_name}] pg_restore command (target: {target['label']})")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)

        # pg_restore exits 1 for warnings; check stderr for real errors
        if result.returncode not in (0, 1):
            raise RuntimeError(f"pg_restore failed (code {result.returncode}):\n{result.stderr[-3000:]}")

        if result.returncode == 1:
            logger.warning(f"[{db_name}] pg_restore completed with warnings (non-fatal)")

    def _create_database_if_not_exists(self, base_url: str, db_name: str, target_type: str):
        """Create target database if it doesn't exist."""
        # Connect to 'postgres' default db
        admin_url = swap_database(base_url, "postgres")
        try:
            conn = psycopg2.connect(admin_url)
            conn.autocommit = True
            cur = conn.cursor()

            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            exists = cur.fetchone()

            if not exists:
                logger.info(f"Creating database '{db_name}' on target...")
                cur.execute(f'CREATE DATABASE "{db_name}"')
            else:
                logger.info(f"Database '{db_name}' already exists on target, will overwrite.")

            cur.close()
            conn.close()
        except Exception as e:
            logger.warning(f"Could not create DB (may already exist or no permission): {e}")

    def _verify_migration(self, db_name: str, target: dict):
        """Verify table count matches between source and target."""
        source_url = swap_database(self.config.source_url, db_name)
        target_url = swap_database(target["url"], db_name)

        def get_table_count(url):
            conn = psycopg2.connect(url)
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*) FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'r'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            """)
            count = cur.fetchone()[0]
            cur.close()
            conn.close()
            return count

        try:
            src_count = get_table_count(source_url)
            tgt_count = get_table_count(target_url)

            if src_count == tgt_count:
                logger.info(f"[{db_name}] ✅ Verification passed: {src_count} tables match")
            else:
                raise ValueError(
                    f"Table count mismatch! Source: {src_count}, Target: {tgt_count}"
                )
        except Exception as e:
            raise RuntimeError(f"Verification failed for {db_name}: {e}")

    def _print_summary(self, state: MigrationState):
        """Print migration summary."""
        success = [s for s in state.databases.values() if s.status == "success"]
        failed = [s for s in state.databases.values() if s.status == "failed"]

        print("\n" + "═" * 60)
        print(f"  MIGRATION SUMMARY — Run: {state.run_id}")
        print("═" * 60)
        print(f"  ✅ Success : {len(success)}")
        print(f"  ❌ Failed  : {len(failed)}")
        print(f"  Total DBs  : {len(state.databases)}")
        print(f"  Duration   : {state.started_at} -> {state.finished_at}")

        if failed:
            print("\n  Failed databases:")
            for s in failed:
                print(f"    - {s.db_name}: {s.error}")
        print("═" * 60 + "\n")

# ─── CLI Entry Point ──────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Neon PostgreSQL Migration Tool")
    p.add_argument("--config", help="Path to YAML config file")
    p.add_argument("--source", help="Source Neon connection URL")
    p.add_argument("--target", help="Target connection URL")
    p.add_argument("--target-type", default="postgres", choices=["neon", "local", "rds", "supabase"])
    p.add_argument("--include-dbs", nargs="*", help="Only migrate these databases")
    p.add_argument("--exclude-dbs", nargs="*", help="Skip these databases")
    p.add_argument("--dump-dir", default="dumps", help="Directory for dump files")
    p.add_argument("--workers", type=int, default=2, help="Parallel workers")
    p.add_argument("--no-verify", action="store_true", help="Skip post-migration verification")
    p.add_argument("--resume", action="store_true", help="Resume from previous run state")
    return p.parse_args()


def main():
    args = parse_args()

    if args.config:
        config = MigrationConfig.from_yaml(args.config)
    elif args.source and args.target:
        config = MigrationConfig.from_args(args)
        if args.include_dbs:
            config.include_dbs = args.include_dbs
        if args.exclude_dbs:
            config.exclude_dbs += args.exclude_dbs
        config.dump_dir = args.dump_dir
        config.parallel_workers = args.workers
        config.verify_after_migrate = not args.no_verify
    else:
        print("ERROR: Provide --config or both --source and --target")
        sys.exit(1)

    # Create or resume state
    if args.resume and STATE_FILE.exists():
        state = MigrationState.load()
        logger.info(f"Resuming run: {state.run_id}")
    else:
        import uuid
        from urllib.parse import urlparse
        state = MigrationState(
            run_id=str(uuid.uuid4())[:8],
            source_host=urlparse(config.source_url).hostname or "unknown",
            target_host="|".join(urlparse(t["url"]).hostname or "?" for t in config.targets),
            target_type="|".join(t["type"] for t in config.targets),
            started_at=datetime.utcnow().isoformat(),
        )

    migrator = NeonMigrator(config)
    success = migrator.run_migration(state)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()