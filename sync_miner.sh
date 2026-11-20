#!/usr/bin/env bash
set -euo pipefail

LOCAL_RAW_DIR="./imports/raw"
mkdir -p "$LOCAL_RAW_DIR"

SSH_KEY="$HOME/.ssh/google_compute_engine"
SSH_OPTS="ssh -i $SSH_KEY -o BatchMode=yes -o ConnectTimeout=15"

# VM 1: Binance (Asia / India)
VM_ASIA_USER="live-miner-asia"
VM_ASIA_IP="IP_EXTERNA_ASIA"
VM_ASIA_PATH="/home/$VM_ASIA_USER/crypto-miner/exports"

# VM 2: Deribit (USA / Oracle)
VM_USA_USER="live-miner-usa"
VM_USA_IP="IP_EXTERNA_USA"
VM_USA_PATH="/home/$VM_USA_USER/crypto-miner/exports"

sync_vm() {
  local label="$1"
  local user="$2"
  local ip="$3"
  local remote_dir="$4"
  local target_dir="$LOCAL_RAW_DIR/$label"

  mkdir -p "$target_dir"
  echo "========================================================"
  echo "Sincronizando [$label] desde ${user}@${ip}:${remote_dir}..."
  echo "========================================================"

  # Comprueba si hay algún .parquet remoto antes de lanzar rsync
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