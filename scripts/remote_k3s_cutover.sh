#!/usr/bin/env bash
set -Eeuo pipefail

mode="${1:?mode is required}"
run_id="${2:?run ID is required}"
production_env="${3:?production env path is required}"
namespace="${4:?namespace is required}"
K3S_PRUNE_DISK_THRESHOLD="${5:-82}"
state_dir="/tmp/bdc-migrate-${run_id}"
production_configmap="$(dirname "$production_env")/k3s/base/configmap.yaml"

deployments=(
  auth-service
  lms-service
  lab-service
  chat-service
  ai-service
  ai-worker
  course-blueprint-worker
)

log() { printf '%s\n' "$*" >&2; }

restore_replicas() {
  [[ -f "${state_dir}/replicas" ]] || { log "Missing replica state: ${state_dir}/replicas"; return 1; }
  while IFS='=' read -r deployment replicas; do
    [[ -n "$deployment" && "$replicas" =~ ^[0-9]+$ ]] || continue
    kubectl -n "$namespace" scale "deployment/${deployment}" --replicas="$replicas" >/dev/null
  done <"${state_dir}/replicas"
}

wait_for_rollouts() {
  while IFS='=' read -r deployment replicas; do
    [[ "$replicas" =~ ^[1-9][0-9]*$ ]] || continue
    kubectl -n "$namespace" rollout status "deployment/${deployment}" --timeout=12m
  done <"${state_dir}/replicas"
}

wait_for_zero_replicas() {
  local deployment current attempts
  for deployment in "${deployments[@]}"; do
    for attempts in {1..150}; do
      current="$(kubectl -n "$namespace" get "deployment/${deployment}" -o jsonpath='{.status.replicas}' 2>/dev/null || true)"
      [[ -z "$current" || "$current" == 0 ]] && break
      sleep 2
    done
    [[ -z "$current" || "$current" == 0 ]] || {
      log "Timed out waiting for ${deployment} to scale to zero"
      return 1
    }
  done
}

safe_k3s_cleanup() {
  local disk_used threshold=${K3S_PRUNE_DISK_THRESHOLD:-82}
  mapfile -t completed < <(kubectl -n "$namespace" get pods --field-selector=status.phase=Succeeded -o name 2>/dev/null || true)
  if ((${#completed[@]})); then
    kubectl -n "$namespace" delete "${completed[@]}" --wait=false >/dev/null
    log "Removed ${#completed[@]} completed pod object(s)."
  fi
  disk_used="$(df -P /var/lib/rancher/k3s 2>/dev/null | awk 'NR==2 {gsub(/%/, "", $5); print $5}')"
  if [[ "$disk_used" =~ ^[0-9]+$ ]] && ((disk_used >= threshold)); then
    log "K3s disk is ${disk_used}%; pruning unused containerd images."
    sudo -n /usr/local/bin/k3s crictl rmi --prune || log "Image prune skipped: approved passwordless sudo rule unavailable."
  else
    log "K3s disk is ${disk_used:-unknown}%; image prune not required."
  fi
}

case "$mode" in
  quiesce)
    [[ -r "$production_env" ]] || { log "Cannot read production env: $production_env"; exit 1; }
    [[ ! -e "$state_dir" ]] || { log "Migration state already exists: $state_dir"; exit 1; }
    umask 077
    mkdir "$state_dir"
    cp "$production_env" "${state_dir}/production.env"
    if [[ -f "$production_configmap" ]]; then
      cp "$production_configmap" "${state_dir}/configmap.source.yaml"
    fi
    kubectl -n "$namespace" get configmap bdc-config -o yaml >"${state_dir}/configmap.yaml"
    kubectl -n "$namespace" get secret bdc-secrets -o yaml >"${state_dir}/secret.yaml"
    : >"${state_dir}/replicas"
    for deployment in "${deployments[@]}"; do
      replicas="$(kubectl -n "$namespace" get "deployment/${deployment}" -o jsonpath='{.spec.replicas}')"
      printf '%s=%s\n' "$deployment" "${replicas:-0}" >>"${state_dir}/replicas"
    done
    for deployment in "${deployments[@]}"; do
      kubectl -n "$namespace" scale "deployment/${deployment}" --replicas=0 >/dev/null
    done
    wait_for_zero_replicas
    log "Production DB writers quiesced; recovery state: $state_dir"
    ;;

  rollback)
    [[ -d "$state_dir" ]] || { log "No rollback state at $state_dir"; exit 1; }
    cp "${state_dir}/production.env" "$production_env"
    if [[ -f "${state_dir}/configmap.source.yaml" ]]; then
      cp "${state_dir}/configmap.source.yaml" "$production_configmap"
    fi
    kubectl -n "$namespace" apply -f "${state_dir}/configmap.yaml" >/dev/null
    kubectl -n "$namespace" apply -f "${state_dir}/secret.yaml" >/dev/null
    for deployment in "${deployments[@]}"; do
      kubectl -n "$namespace" rollout restart "deployment/${deployment}" >/dev/null
    done
    restore_replicas
    wait_for_rollouts
    rm -rf -- "$state_dir"
    log "Production source configuration and replicas restored."
    ;;

  cutover)
    : "${DESTINATION_URL:?DESTINATION_URL is required}"
    [[ -d "$state_dir" ]] || {
      # --no-quiesce still needs a replica/config snapshot for rollback.
      umask 077
      mkdir "$state_dir"
      cp "$production_env" "${state_dir}/production.env"
      if [[ -f "$production_configmap" ]]; then
        cp "$production_configmap" "${state_dir}/configmap.source.yaml"
      fi
      kubectl -n "$namespace" get configmap bdc-config -o yaml >"${state_dir}/configmap.yaml"
      kubectl -n "$namespace" get secret bdc-secrets -o yaml >"${state_dir}/secret.yaml"
      : >"${state_dir}/replicas"
      for deployment in "${deployments[@]}"; do
        replicas="$(kubectl -n "$namespace" get "deployment/${deployment}" -o jsonpath='{.spec.replicas}')"
        printf '%s=%s\n' "$deployment" "${replicas:-0}" >>"${state_dir}/replicas"
      done
    }

    DESTINATION_URL="$DESTINATION_URL" python3 - "$production_env" "$state_dir" <<'PY'
import base64
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from urllib.parse import unquote, urlsplit

env_path = Path(sys.argv[1])
state_dir = Path(sys.argv[2])
configmap_path = env_path.parent / "k3s" / "base" / "configmap.yaml"
url = urlsplit(os.environ["DESTINATION_URL"])
if url.scheme not in {"postgresql", "postgres"} or not url.hostname:
    raise SystemExit("Invalid destination PostgreSQL URL")
if url.username is None or url.password is None:
    raise SystemExit("Destination URL must include user and password")
if not url.path.lstrip("/"):
    raise SystemExit("Destination URL must include an administrative database path")
if url.port not in (None, 5432):
    raise SystemExit("Production K3s manifests require PostgreSQL port 5432")

host_keys = ["POSTGRES_HOST", "LMS_POSTGRES_HOST", "LAB_POSTGRES_HOST", "CHAT_POSTGRES_HOST", "AI_POSTGRES_HOST", "DUTYLOG_POSTGRES_HOST"]
user_keys = ["POSTGRES_USER", "LMS_POSTGRES_USER", "LAB_POSTGRES_USER", "CHAT_POSTGRES_USER", "AI_POSTGRES_USER", "DUTYLOG_POSTGRES_USER"]
password_keys = ["POSTGRES_PASSWORD", "LMS_POSTGRES_PASSWORD", "LAB_POSTGRES_PASSWORD", "CHAT_POSTGRES_PASSWORD", "AI_POSTGRES_PASSWORD", "DUTYLOG_POSTGRES_PASSWORD"]
replacements = {key: url.hostname for key in host_keys}
replacements.update({key: unquote(url.username) for key in user_keys})
replacements.update({key: unquote(url.password) for key in password_keys})

content = env_path.read_text(encoding="utf-8")
lines = content.splitlines(keepends=True)
last_index = {}
for index, line in enumerate(lines):
    match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=", line)
    if match and match.group(1) in replacements:
        last_index[match.group(1)] = index
for key, index in last_index.items():
    lines[index] = f"{key}={replacements[key]}" + ("\n" if lines[index].endswith("\n") else "")
missing = sorted(set(replacements) - set(last_index))
if missing:
    lines.append(("" if content.endswith("\n") else "\n") + "\n".join(f"{k}={replacements[k]}" for k in missing) + "\n")
fd, temporary = tempfile.mkstemp(prefix=".bdc-env.", dir=env_path.parent)
try:
    os.fchmod(fd, env_path.stat().st_mode & 0o777)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write("".join(lines))
    os.replace(temporary, env_path)
except BaseException:
    try:
        os.unlink(temporary)
    except FileNotFoundError:
        pass
    raise

if configmap_path.is_file():
    configmap = configmap_path.read_text(encoding="utf-8")
    for key in host_keys:
        pattern = re.compile(rf'^(\s*{re.escape(key)}:\s*)"[^"]*"\s*$', re.MULTILINE)
        configmap, count = pattern.subn(rf'\1"{url.hostname}"', configmap)
        if count != 1:
            raise SystemExit(f"Expected one {key} in {configmap_path}, found {count}")
    fd, temporary = tempfile.mkstemp(prefix=".bdc-configmap.", dir=configmap_path.parent)
    try:
        os.fchmod(fd, configmap_path.stat().st_mode & 0o777)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(configmap)
        os.replace(temporary, configmap_path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise

encode = lambda value: base64.b64encode(value.encode()).decode()
config_patch = {"data": {key: url.hostname for key in host_keys}}
secret_patch = {"data": {}}
for key in user_keys:
    secret_patch["data"][key] = encode(unquote(url.username))
for key in password_keys:
    secret_patch["data"][key] = encode(unquote(url.password))
(state_dir / "config-patch.json").write_text(json.dumps(config_patch), encoding="utf-8")
(state_dir / "secret-patch.json").write_text(json.dumps(secret_patch), encoding="utf-8")
PY

    kubectl -n "$namespace" patch configmap bdc-config --type=merge --patch-file "${state_dir}/config-patch.json" >/dev/null
    kubectl -n "$namespace" patch secret bdc-secrets --type=merge --patch-file "${state_dir}/secret-patch.json" >/dev/null

    # Redis also reads bdc-secrets. Restart it before clients so both sides use
    # the same runtime credentials even if the source Secret had been repaired.
    kubectl -n "$namespace" rollout restart statefulset/redis >/dev/null
    kubectl -n "$namespace" rollout status statefulset/redis --timeout=5m

    for deployment in "${deployments[@]}"; do
      kubectl -n "$namespace" rollout restart "deployment/${deployment}" >/dev/null
    done
    restore_replicas
    wait_for_rollouts

    safe_k3s_cleanup

    rm -rf -- "$state_dir"
    kubectl -n "$namespace" get deployment \
      auth-service lms-service lab-service chat-service ai-service ai-worker course-blueprint-worker
    log "Production K3s cutover completed."
    ;;

  *)
    log "Unknown mode: $mode"
    exit 2
    ;;
esac
