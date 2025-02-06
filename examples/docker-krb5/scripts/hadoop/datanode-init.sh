#!/bin/bash

# Wait until the Namenode server is accepting connections on port 9871 using bash's /dev/tcp
while ! bash -c "echo > /dev/tcp/namenode/9871" &> /dev/null; do
  echo "Waiting for Namenode server to come online on port 9871..."
  sleep 2
done

export HOSTNAME=$(nslookup $(hostname -i) | awk -F 'name = ' '/name =/ {print $2}' | sed 's/\.$//')
export KRB5_PRINC=dn/${HOSTNAME}@${KRB5_REALM}
export KRB5_KEYTAB_PATH=${KRB5_HADOOP_KEYTABS}/dn.service.keytab
export SSL_ALIAS=$HOSTNAME

source $(dirname $0)/common-functions.sh

create_internal_directories
create_kerberos_keytabs
create_tls_keystore

# Ensure the DataNode data directory has the correct permissions
mkdir -p /opt/hadoop/hadoop_data/datanode
chmod 700 /opt/hadoop/hadoop_data/datanode

# Start Datanode
hdfs datanode -Ddfs.datanode.hostname=${HOSTNAME}
