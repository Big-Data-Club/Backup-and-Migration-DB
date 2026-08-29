#!/bin/sh
set -eu

: "${SOURCE_URL:?SOURCE_URL is required}"
: "${TARGET_URL:?TARGET_URL is required}"
: "${TARGET_DIRECT_URL:?TARGET_DIRECT_URL is required}"
: "${MIGRATION_JOBS:=2}"
: "${RUN_ID:?RUN_ID is required}"
: "${DUMP_ROOT:=/dumps}"

umask 077
mkdir -p "$DUMP_ROOT"
report="${DUMP_ROOT}/report.tsv"
databases_file="${DUMP_ROOT}/databases.txt"

database_url() {
  url=$1
  database=$2
  case "$url" in
    *\?*) base=${url%%\?*}; query=?${url#*\?} ;;
    *) base=$url; query= ;;
  esac
  printf '%s/%s%s' "${base%/*}" "$database" "$query"
}

psql "$SOURCE_URL" -v ON_ERROR_STOP=1 -Atc \
  "SELECT datname FROM pg_database WHERE datistemplate=false AND datallowconn=true AND datname NOT IN ('postgres') ORDER BY datname" \
  >"$databases_file"

if [ ! -s "$databases_file" ]; then
  echo "No user databases discovered on source" >&2
  exit 1
fi

printf 'database\tsource_tables\ttarget_tables\tdump_bytes\tstatus\n' >"$report"
echo "Discovered databases: $(tr '\n' ' ' <"$databases_file")"

while IFS= read -r database; do
  [ -n "$database" ] || continue
  case "$database" in
    *[!A-Za-z0-9_.-]*)
      echo "Unsupported database name for filesystem-safe dumps: $database" >&2
      exit 1
      ;;
  esac
  source_db_url=$(database_url "$SOURCE_URL" "$database")
  target_db_url=$(database_url "$TARGET_URL" "$database")
  dump_path="${DUMP_ROOT}/${database}.directory"

  echo "[$database] dumping with ${MIGRATION_JOBS} job(s)"
  rm -rf -- "$dump_path"
  pg_dump \
    --format=directory \
    --jobs="$MIGRATION_JOBS" \
    --no-owner \
    --no-acl \
    --file="$dump_path" \
    "$source_db_url"

  exists=$(psql "$TARGET_URL" -v database="$database" -Atc \
    "SELECT 1 FROM pg_database WHERE datname=:'database'")
  if [ "$exists" != 1 ]; then
    echo "[$database] creating target database"
    createdb --maintenance-db="$TARGET_URL" "$database"
  fi

  echo "[$database] restoring"
  pg_restore \
    --jobs="$MIGRATION_JOBS" \
    --no-owner \
    --no-acl \
    --clean \
    --if-exists \
    --exit-on-error \
    --dbname="$target_db_url" \
    "$dump_path"

  # A source database can carry an empty database-level search_path. Reset it
  # after restore; the owner role default is established once below.
  psql "$TARGET_URL" -v ON_ERROR_STOP=1 -v database="$database" <<'SQL' >/dev/null
SELECT format('ALTER DATABASE %I RESET search_path', :'database') \gexec
SQL

  source_tables=$(psql "$source_db_url" -Atc \
    "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='r' AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')")
  target_tables=$(psql "$target_db_url" -Atc \
    "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='r' AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')")
  if [ "$source_tables" != "$target_tables" ]; then
    printf '%s\t%s\t%s\t%s\tfailed\n' "$database" "$source_tables" "$target_tables" 0 >>"$report"
    echo "[$database] table verification failed: source=$source_tables target=$target_tables" >&2
    exit 1
  fi
  dump_bytes=$(du -sk "$dump_path" | awk '{print $1 * 1024}')
  printf '%s\t%s\t%s\t%s\tsuccess\n' "$database" "$source_tables" "$target_tables" "$dump_bytes" >>"$report"
  echo "[$database] verified: ${target_tables} user tables"
done <"$databases_file"

psql "$TARGET_URL" -v ON_ERROR_STOP=1 <<'SQL' >/dev/null
SELECT format('ALTER ROLE %I SET search_path TO "$user", public', current_user) \gexec
SQL

# Poolers may retain backend sessions created while a restored database-level
# setting was active. Use the direct endpoint to recycle only this owner's
# sessions, then verify a fresh pooled connection sees the standard schema.
terminated=$(psql "$TARGET_DIRECT_URL" -v ON_ERROR_STOP=1 -Atc \
  "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename=current_user AND pid<>pg_backend_pid()) AS recycled")
echo "Recycled ${terminated} destination backend session(s)."

while IFS= read -r database; do
  [ -n "$database" ] || continue
  target_db_url=$(database_url "$TARGET_URL" "$database")
  current_schema=$(psql "$target_db_url" -Atc "SELECT current_schema()")
  if [ "$current_schema" != public ]; then
    echo "[$database] pooled search_path verification failed: current_schema=${current_schema:-<empty>}" >&2
    exit 1
  fi
done <"$databases_file"

echo "ALL_DATABASES_MIGRATED run_id=${RUN_ID}"
