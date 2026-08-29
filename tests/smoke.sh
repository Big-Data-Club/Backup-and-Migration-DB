#!/usr/bin/env bash
set -Eeuo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture="$(mktemp -d)"
run_id="smoke-${RANDOM}-${RANDOM}"
rollback_run_id="rollback-${RANDOM}-${RANDOM}"
trap 'rm -rf -- "$fixture" "/tmp/bdc-migrate-${run_id}" "/tmp/bdc-migrate-${rollback_run_id}"' EXIT

mkdir -p "$fixture/core/k3s/base" "$fixture/bin"

seed_core_fixture() {
  cat >"$fixture/core/.env" <<'EOF'
POSTGRES_USER=local_user
POSTGRES_PASSWORD=local_password
POSTGRES_HOST=local-postgres
POSTGRES_HOST=old-pooler.example.test
POSTGRES_USER=old_user
POSTGRES_PASSWORD=old_password
LMS_POSTGRES_HOST=old-pooler.example.test
LMS_POSTGRES_USER=old_user
LMS_POSTGRES_PASSWORD=old_password
LAB_POSTGRES_HOST=old-pooler.example.test
LAB_POSTGRES_USER=old_user
LAB_POSTGRES_PASSWORD=old_password
CHAT_POSTGRES_HOST=old-pooler.example.test
CHAT_POSTGRES_USER=old_user
CHAT_POSTGRES_PASSWORD=old_password
AI_POSTGRES_HOST=old-pooler.example.test
AI_POSTGRES_USER=old_user
AI_POSTGRES_PASSWORD=old_password
EOF
  cat >"$fixture/core/k3s/base/configmap.yaml" <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: bdc-config
data:
  POSTGRES_HOST: "old-pooler.example.test"
  LMS_POSTGRES_HOST: "old-pooler.example.test"
  LAB_POSTGRES_HOST: "old-pooler.example.test"
  CHAT_POSTGRES_HOST: "old-pooler.example.test"
  AI_POSTGRES_HOST: "old-pooler.example.test"
EOF
}

seed_core_fixture

DESTINATION_URL='postgresql://fixture_user:fixture_password@fixture-pooler.example.test/ai?sslmode=require' \
  python3 "$root_dir/scripts/update_core_config.py" "$fixture/core" >/dev/null

last_value() {
  awk -F= -v wanted="$1" '$1==wanted {value=substr($0,index($0,"=")+1)} END{print value}' "$fixture/core/.env"
}

for prefix in POSTGRES LMS_POSTGRES LAB_POSTGRES CHAT_POSTGRES AI_POSTGRES; do
  [[ "$(last_value "${prefix}_HOST")" == fixture-pooler.example.test ]]
  [[ "$(last_value "${prefix}_USER")" == fixture_user ]]
  [[ "$(last_value "${prefix}_PASSWORD")" == fixture_password ]]
done
[[ "$(grep -c 'fixture-pooler.example.test' "$fixture/core/k3s/base/configmap.yaml")" == 5 ]]

cat >"$fixture/bin/kubectl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
args="$*"
case "$args" in
  *"get configmap bdc-config -o yaml"*)
    printf '%s\n' 'apiVersion: v1' 'kind: ConfigMap' 'metadata: {name: bdc-config}' 'data: {}'
    ;;
  *"get secret bdc-secrets -o yaml"*)
    printf '%s\n' 'apiVersion: v1' 'kind: Secret' 'metadata: {name: bdc-secrets}' 'data: {}'
    ;;
  *"get deployment/"*"jsonpath={.spec.replicas}"*) printf '1' ;;
  *"get deployment/"*"jsonpath={.status.replicas}"*) printf '0' ;;
  *"get pods --field-selector=status.phase=Succeeded"*) : ;;
  *) : ;;
esac
MOCK
cat >"$fixture/bin/df" <<'MOCK'
#!/usr/bin/env sh
printf '%s\n' 'Filesystem 1024-blocks Used Available Capacity Mounted on' '/dev/mock 100 70 30 70% /'
MOCK
cat >"$fixture/bin/psql" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
args="$*"
case "$args" in
  *"datistemplate=false"*) printf '%s\n' ai auth ;;
  *"datname=:'database'"*) printf '1\n' ;;
  *"count(*) FROM pg_class"*) printf '2\n' ;;
  *"pg_terminate_backend"*) printf '0\n' ;;
  *"current_schema()"*) printf 'public\n' ;;
  *) cat >/dev/null || true ;;
esac
MOCK
cat >"$fixture/bin/pg_dump" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
for argument in "$@"; do
  case "$argument" in
    --file=*) destination=${argument#--file=} ;;
  esac
done
mkdir -p "${destination:?missing --file}"
printf 'fixture dump\n' >"${destination}/toc.dat"
MOCK
cat >"$fixture/bin/pg_restore" <<'MOCK'
#!/usr/bin/env sh
exit 0
MOCK
cat >"$fixture/bin/createdb" <<'MOCK'
#!/usr/bin/env sh
exit 0
MOCK
chmod +x "$fixture/bin/kubectl" "$fixture/bin/df" "$fixture/bin/psql" \
  "$fixture/bin/pg_dump" "$fixture/bin/pg_restore" "$fixture/bin/createdb"

SOURCE_URL='postgresql://source:secret@source.example.test/ai?sslmode=require' \
TARGET_URL='postgresql://target:secret@target-pooler.example.test/ai?sslmode=require' \
TARGET_DIRECT_URL='postgresql://target:secret@target.example.test/ai?sslmode=require' \
MIGRATION_JOBS=2 RUN_ID=fixture DUMP_ROOT="$fixture/dumps" \
PATH="$fixture/bin:$PATH" sh "$root_dir/scripts/migrate_all_databases.sh" >/dev/null
[[ "$(grep -c $'\tsuccess$' "$fixture/dumps/report.tsv")" == 2 ]]

# Verify rollback restores the exact persistent env and ConfigMap files.
seed_core_fixture
cp "$fixture/core/.env" "$fixture/original.env"
cp "$fixture/core/k3s/base/configmap.yaml" "$fixture/original-configmap.yaml"
PATH="$fixture/bin:$PATH" bash "$root_dir/scripts/remote_k3s_cutover.sh" \
  quiesce "$rollback_run_id" "$fixture/core/.env" default 82
DESTINATION_URL='postgresql://changed:changed@changed-pooler.example.test/ai?sslmode=require' \
  python3 "$root_dir/scripts/update_core_config.py" "$fixture/core" >/dev/null
PATH="$fixture/bin:$PATH" bash "$root_dir/scripts/remote_k3s_cutover.sh" \
  rollback "$rollback_run_id" "$fixture/core/.env" default 82
cmp "$fixture/original.env" "$fixture/core/.env"
cmp "$fixture/original-configmap.yaml" "$fixture/core/k3s/base/configmap.yaml"

# The remote script is exercised with mocked cluster commands. This validates
# its state capture, embedded Python config update, rollout path, and cleanup
# without touching a real cluster.
seed_core_fixture
PATH="$fixture/bin:$PATH" bash "$root_dir/scripts/remote_k3s_cutover.sh" \
  quiesce "$run_id" "$fixture/core/.env" default 82
DESTINATION_URL='postgresql://fixture_user:fixture_password@fixture-pooler.example.test/ai?sslmode=require' \
  PATH="$fixture/bin:$PATH" bash "$root_dir/scripts/remote_k3s_cutover.sh" \
  cutover "$run_id" "$fixture/core/.env" default 82

[[ ! -e "/tmp/bdc-migrate-${run_id}" ]]
[[ "$(last_value POSTGRES_HOST)" == fixture-pooler.example.test ]]
[[ "$(last_value AI_POSTGRES_PASSWORD)" == fixture_password ]]

bash -n "$root_dir/start_migrate"
sh -n "$root_dir/scripts/migrate_all_databases.sh"
bash -n "$root_dir/scripts/remote_k3s_cutover.sh"
python3 -m py_compile "$root_dir/scripts/update_core_config.py"

echo "Backup-and-Migration-DB smoke tests passed."
