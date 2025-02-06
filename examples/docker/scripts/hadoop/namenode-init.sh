#!/bin/bash
set -xe

# Set hostname variables
export HOSTNAME=$(hostname)
export FQDN=$(hostname -f)
export SSL_ALIAS=$HOSTNAME

# Source common functions
source $(dirname $0)/common-functions.sh

# Create necessary directories
create_internal_directories
create_tls_keystore

echo "Formatting HDFS Filesystem..."
# Format HDFS Filesystem with proper error handling
hdfs namenode -Dfs.defaultFS=hdfs://${FQDN} -format || {
  echo "Error formatting namenode. Check logs for details." >&2
  exit 1
}

echo "Starting Namenode..."
# Start Namenode with proper configuration
exec hdfs namenode -Dfs.defaultFS=hdfs://${FQDN} -Ddfs.namenode.rpc-bind-host=0.0.0.0 -Ddfs.namenode.servicerpc-bind-host=0.0.0.0 -Ddfs.namenode.http-bind-host=0.0.0.0 -Ddfs.namenode.https-bind-host=0.0.0.0
