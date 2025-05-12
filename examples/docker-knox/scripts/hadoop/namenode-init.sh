#!/bin/bash
set -xe

# Wait until the Kerberos server is accepting connections on port 88 using bash's /dev/tcp
while ! bash -c "echo > /dev/tcp/kerberos-server/88" &> /dev/null; do
  echo "Waiting for Kerberos server to come online on port 88..."
  sleep 2
done

KRB5_PRINC=nn/$HOSTNAME@${KRB5_REALM}
KRB5_HTTP_PRINC=HTTP/$HOSTNAME@${KRB5_REALM}
KRB5_KEYTAB_PATH=${KRB5_HADOOP_KEYTABS}/nn.service.keytab
SSL_ALIAS=$HOSTNAME

source $(dirname $0)/common-functions.sh

create_internal_directories
create_kerberos_keytabs
create_tls_keystore

# Format HDFS Filesystem
hdfs namenode -Dfs.defaultFS=hdfs://$(hostname) -format

# Start Namenode
hdfs namenode -Dfs.defaultFS=hdfs://$(hostname)
