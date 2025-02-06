#!/bin/bash

export HOSTNAME=$(nslookup $(hostname -i) | awk -F 'name = ' '/name =/ {print $2}' | sed 's/\.$//')
export SSL_ALIAS=$HOSTNAME

source $(dirname $0)/common-functions.sh

create_internal_directories
create_tls_keystore

# Start Datanode
hdfs datanode -Ddfs.datanode.hostname=${HOSTNAME}
