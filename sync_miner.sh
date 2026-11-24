#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${MINER_ENV_FILE:-./.env}"
if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  set -a
  source "$ENV_FILE"
  set +a
fi

: "${VM_ASIA_USER:?Falta definir VM_ASIA_USER en $ENV_FILE}"
: "${VM_ASIA_IP:?Falta definir VM_ASIA_IP en $ENV_FILE}"
: "${VM_ASIA_PATH:?Falta definir VM_ASIA_PATH en $ENV_FILE}"
: "${VM_USA_USER:?Falta definir VM_USA_USER en $ENV_FILE}"
: "${VM_USA_IP:?Falta definir VM_USA_IP en $ENV_FILE}"
: "${VM_USA_PATH:?Falta definir VM_USA_PATH en $ENV_FILE}"

LOCAL_RAW_DIR="${LOCAL_RAW_DIR:-./imports/raw}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/google_compute_engine}"
SSH_OPTS="ssh -i $SSH_KEY -o BatchMode=yes -o ConnectTimeout=15"

mkdir -p "$LOCAL_RAW_DIR"

sync_vm() {
  local label="$1"
  local user="$2"
  local ip="$3"
  local remote_dir="$4"
  local target_dir="$LOCAL_RAW_DIR/$label"

  mkdir -p "$target_dir"
  echo "========================================================"
  echo "Sincronizando [$label] (${user}@${ip})..."
  echo "========================================================"

  if ! $SSH_OPTS "${user}@${ip}" "ls ${remote_dir}/*.parquet >/dev/null 2>&1"; then
    echo "ℹ No hay archivos .parquet nuevos en [$label]."
    return 0
  fi

  rsync -avz --remove-source-files \
    --include="*.parquet" --exclude="*" \
    -e "$SSH_OPTS" \
    "${user}@${ip}:${remote_dir}/" "$target_dir/"

  echo "✔ [$label] sincronizado correctamente."
}

sync_vm "asia" "$VM_ASIA_USER" "$VM_ASIA_IP" "$VM_ASIA_PATH"
sync_vm "usa"  "$VM_USA_USER"  "$VM_USA_IP"  "$VM_USA_PATH"

echo ""
echo "Descarga completada en: $LOCAL_RAW_DIR"