#!/bin/bash

function create_internal_directories() {
    sudo mkdir -p ${SSL_KEYSTORES_PATH}
    sudo chown ${HADOOP_USER}:${HADOOP_GROUP} -R ${SSL_KEYSTORES_PATH}
    sudo chown ${HADOOP_USER}:${HADOOP_GROUP} -R ${SSL_TRUSTSTORES_PATH}
    sudo chown ${HADOOP_USER}:${HADOOP_GROUP} -R ${SCRIPTS_PATH}
    sudo chmod +x -R ${SCRIPTS_PATH}
}


function create_kerberos_keytabs() {

    # FIXME : Create an empty/dumb keytab
    echo -e "\0005\0002\c" > ${KRB5_KEYTAB_PATH}

    kadmin  -d 5 -w ${KRB5_ADMIN_USER_PASSWD} -p admin/admin@${KRB5_REALM} \
    -q "addprinc -e  aes256-cts -randkey ${KRB5_PRINC}"
    kadmin  -d 5 -w ${KRB5_ADMIN_USER_PASSWD} -p admin/admin@${KRB5_REALM} \
    -q "ktadd -e  aes256-cts -k ${KRB5_KEYTAB_PATH} ${KRB5_PRINC}"

    if [ -v KRB5_HTTP_PRINC ]; then
        # HTTP SPN
        kadmin  -d 5   -w ${KRB5_ADMIN_USER_PASSWD} -p admin/admin@${KRB5_REALM} \
        -q "addprinc -e  aes256-cts -randkey ${KRB5_HTTP_PRINC}"
        kadmin  -d 5   -w ${KRB5_ADMIN_USER_PASSWD} -p admin/admin@${KRB5_REALM} \
        -q "ktadd -e  aes256-cts -k ${KRB5_KEYTAB_PATH} ${KRB5_HTTP_PRINC}"
    fi

    klist -kt ${KRB5_KEYTAB_PATH} -e

}

function create_tls_keystore() {
    # Generate a Keystore with a PrivateKey/Certificate
    if [ -f "${SSL_KEYSTORE_PATH}" ]; then
      rm -f "${SSL_KEYSTORE_PATH}"
    fi
    if [ -f "${SSL_KEYSTORES_PATH}/${SSL_ALIAS}.cer" ]; then
      rm -f "${SSL_KEYSTORES_PATH}/${SSL_ALIAS}.cer"
    fi

    # First, delete the certificate if it exists in the truststore
    keytool -delete -alias ${SSL_ALIAS} -keystore ${SSL_TRUSTSTORE_PATH} -storepass ${STORE_PASSWD} 2>/dev/null || true
    keytool -delete -alias ${SSL_ALIAS} -keystore ${SSL_TRUSTSTORES_PATH}/truststore.p12 -storepass ${STORE_PASSWD} 2>/dev/null || true

    # Generate the keypair with proper SAN extension
    keytool -genkeypair -alias ${SSL_ALIAS} \
        -keyalg EC -keysize 256 \
        -sigalg SHA256withECDSA \
        -dname "CN=${HOSTNAME},OU=LOCAL,O=HADOOP" \
        -ext "SAN=DNS:${HOSTNAME},DNS:${EXTERNAL_HOSTNAME}" \
        -validity 365 \
        -keypass ${STORE_PASSWD} -storepass ${STORE_PASSWD} \
        -keystore ${SSL_KEYSTORE_PATH} \
        -storetype PKCS12

    # Self-sign the key pair with the same SAN extension
    keytool -selfcert -alias ${SSL_ALIAS} \
         -validity 365 \
         -ext "SAN=DNS:${HOSTNAME},DNS:${EXTERNAL_HOSTNAME}" \
         -keypass ${STORE_PASSWD} -storepass ${STORE_PASSWD} \
         -keystore ${SSL_KEYSTORE_PATH}

    # Export Service Certificate
    keytool -exportcert -v -alias ${SSL_ALIAS} \
            -file ${SSL_KEYSTORES_PATH}/${SSL_ALIAS}.cer \
            -keystore ${SSL_KEYSTORE_PATH} \
            -storepass ${STORE_PASSWD}

    # Delete the certificate if it exists in the truststore
    keytool -delete -alias ${SSL_ALIAS} -keystore ${SSL_TRUSTSTORE_PATH} -storepass ${STORE_PASSWD} 2>/dev/null || true

    # Import Service Certificate into Trusted Hadoop certificates with -noprompt
    keytool -importcert -alias ${SSL_ALIAS} \
            -file ${SSL_KEYSTORES_PATH}/${SSL_ALIAS}.cer \
            -keypass ${STORE_PASSWD} -storepass ${STORE_PASSWD} \
            -keystore ${SSL_TRUSTSTORE_PATH} \
            -storetype PKCS12 -noprompt

    # Show Keystore Content
    echo -e "\n\n ==================== Keystore Content : ================================="
    keytool -keystore ${SSL_KEYSTORE_PATH} -storepass ${STORE_PASSWD} -v -list

    # Show Trustore Content
    echo -e "\n\n ==================== Trustore Content : ================================="
    keytool -keystore ${SSL_TRUSTSTORE_PATH} -storepass ${STORE_PASSWD} -v -list
}
