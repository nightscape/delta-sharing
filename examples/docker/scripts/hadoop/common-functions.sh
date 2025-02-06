#!/bin/bash

function create_internal_directories() {
    sudo mkdir -p ${SSL_KEYSTORES_PATH}
    sudo chown ${HADOOP_USER}:${HADOOP_GROUP} -R ${SSL_KEYSTORES_PATH}
    sudo chown ${HADOOP_USER}:${HADOOP_GROUP} -R ${SSL_TRUSTSTORES_PATH}
    sudo chown ${HADOOP_USER}:${HADOOP_GROUP} -R ${SCRIPTS_PATH}
    sudo chmod +x -R ${SCRIPTS_PATH}
}


function create_tls_keystore() {

    # Generate a Keystore with a PrivateKey/Certificate
    if [ -f "${SSL_KEYSTORE_PATH}" ]; then
      rm -f "${SSL_KEYSTORE_PATH}"
    fi
    if [ -f "${SSL_KEYSTORES_PATH}/${SSL_ALIAS}.cer" ]; then
      rm -f "${SSL_KEYSTORES_PATH}/${SSL_ALIAS}.cer"
    fi
    keytool -genkeypair -alias ${SSL_ALIAS} \
        -keyalg EC -keysize 256 \
        -sigalg SHA256withECDSA \
        -dname "CN=${HOSTNAME},OU=LOCAL,O=HADOOP" \
        -validity 365 \
        -keypass ${STORE_PASSWD} -storepass ${STORE_PASSWD} \
        -keystore ${SSL_KEYSTORE_PATH} \
        -storetype PKCS12

    # Self-sign the key pair to ensure a certificate is present
    keytool -selfcert -alias ${SSL_ALIAS} \
         -validity 365 \
         -keypass ${STORE_PASSWD} -storepass ${STORE_PASSWD} \
         -keystore ${SSL_KEYSTORE_PATH}

    # Export Service Certificate
    keytool -exportcert -v -alias ${SSL_ALIAS} \
            -file ${SSL_KEYSTORES_PATH}/${SSL_ALIAS}.cer \
            -keystore ${SSL_KEYSTORE_PATH} \
            -storepass ${STORE_PASSWD}

    # Import Service Certificate into Trusted Hadoop certificates
    keytool  -importcert -alias ${SSL_ALIAS} \
            -file ${SSL_KEYSTORES_PATH}/${SSL_ALIAS}.cer \
            -keypass ${STORE_PASSWD} -storepass ${STORE_PASSWD} \
            -keystore ${SSL_TRUSTSTORE_PATH} \
            -storetype PKCS12 -noprompt

    # Show Keystore Content
    echo -e "\n\n ==================== Keystore Content : ================================="
    echo ${STORE_PASSWD} | keytool -keystore ${SSL_KEYSTORE_PATH} -v -list

    # Show Trustore Content
    echo -e "\n\n ==================== Trustore Content : ================================="
    echo ${STORE_PASSWD} | keytool -keystore ${SSL_TRUSTSTORE_PATH} -v -list

}
