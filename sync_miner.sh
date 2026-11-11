#!/bin/bash
# Syncs Parquet files from the VM and instantly deletes them from the remote server
VM_USER="ubuntu"
VM_IP="your.vm.ip.address"
REMOTE_DIR="/home/ubuntu/crypto-miner/exports/"
LOCAL_DIR="./imports/"

mkdir -p $LOCAL_DIR

echo "Fetching Parquet files from Edge VM..."
rsync -avz --remove-source-files -e "ssh -i ~/.ssh/your_vm_key.pem" ${VM_USER}@${VM_IP}:${REMOTE_DIR}*.parquet $LOCAL_DIR

echo "Sync complete. Remote files deleted."