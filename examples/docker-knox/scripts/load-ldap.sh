#!/bin/bash

# This script loads the users.ldif file into the embedded LDAP server.

# Determine the LDAP server type and connection parameters.
# This is a guess, you may need to adjust these values based on the
# actual LDAP server used in the farberg/apache-knox-docker:1.5.0 image.

LDAP_HOST="localhost"
LDAP_PORT="33389" # Default LDAP port, check if this is correct
LDAP_USER="cn=admin,dc=hadoop,dc=local" # Or similar admin DN
LDAP_PASSWORD="admin" # Or the correct admin password

# Try ldapmodify to import the users.ldif file.
# You may need to adjust the ldapmodify command based on the LDAP server.
ldapmodify -H ldap://${LDAP_HOST}:${LDAP_PORT} -x -D "${LDAP_USER}" -w "${LDAP_PASSWORD}" -f /opt/knox-1.5.0/conf/users.ldif -c

# Check if the import was successful.
if [ $? -eq 0 ]; then
  echo "Successfully loaded users.ldif into LDAP."
else
  echo "Failed to load users.ldif into LDAP."
  exit 1
fi
