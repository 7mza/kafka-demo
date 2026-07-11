#!/bin/bash
set -eu

: "${CA_KEY_PASSWORD:?CA_KEY_PASSWORD not set in .env}"
: "${PKCS12_PASSWORD:?PKCS12_PASSWORD not set in .env}"
: "${SECRETS_DIR:?SECRETS_DIR not set in .env}"

CERTS_DIR=./certs
CA_DAYS=3650
CERT_DAYS=3650

mkdir -p "${CERTS_DIR:?}"

if [ "${FORCE_REGEN:-false}" = "true" ]; then
        rm -f "${CERTS_DIR:?}"/* "${CERTS_DIR:?}/.generated"
fi

if [ -f "${CERTS_DIR:?}/.generated" ]; then
        echo "skipping: certs already generated (set .env FORCE_REGEN=true to regenerate)"
        exit 0
fi

generate_ca() {
        keytool -genkeypair \
                -alias rootCA \
                -dname "CN=rootCA" \
                -keyalg RSA -keysize 4096 \
                -validity "${CA_DAYS}" \
                -ext "bc:c=ca:true" \
                -ext "ku:c=keyCertSign,cRLSign" \
                -keystore "${CERTS_DIR:?}/ca.p12" \
                -storetype PKCS12 \
                -storepass "${CA_KEY_PASSWORD}" \
                -keypass "${CA_KEY_PASSWORD}"

        keytool -exportcert -rfc \
                -alias rootCA \
                -keystore "${CERTS_DIR:?}/ca.p12" \
                -storepass "${CA_KEY_PASSWORD}" \
                -file "${CERTS_DIR:?}/rootCA.pem"
}

generate_truststore() {
        keytool -importcert -noprompt \
                -alias rootCA \
                -file "${CERTS_DIR:?}/rootCA.pem" \
                -keystore "${CERTS_DIR:?}/truststore.p12" \
                -storetype PKCS12 \
                -storepass "${PKCS12_PASSWORD}"
}

generate_identity() {
        local name=$1
        local dns=$2
        local p12="${CERTS_DIR:?}/${name}.p12"

        keytool -genkeypair \
                -alias "${name}" \
                -dname "CN=${name}" \
                -keyalg RSA -keysize 4096 \
                -validity "${CERT_DAYS}" \
                -keystore "${p12}" \
                -storetype PKCS12 \
                -storepass "${PKCS12_PASSWORD}" \
                -keypass "${PKCS12_PASSWORD}"

        keytool -certreq \
                -alias "${name}" \
                -keystore "${p12}" \
                -storepass "${PKCS12_PASSWORD}" \
                -keypass "${PKCS12_PASSWORD}" \
                -file "${CERTS_DIR:?}/${name}.csr"

        keytool -gencert \
                -alias rootCA \
                -keystore "${CERTS_DIR:?}/ca.p12" \
                -storepass "${CA_KEY_PASSWORD}" \
                -keypass "${CA_KEY_PASSWORD}" \
                -infile "${CERTS_DIR:?}/${name}.csr" \
                -validity "${CERT_DAYS}" \
                -ext "ku:c=digitalSignature,keyEncipherment" \
                -ext "eku=clientAuth,serverAuth" \
                -ext "san=${dns},DNS:localhost,IP:127.0.0.1" \
                -rfc \
                -outfile "${CERTS_DIR:?}/${name}.pem"

        keytool -importcert -noprompt \
                -alias rootCA \
                -file "${CERTS_DIR:?}/rootCA.pem" \
                -keystore "${p12}" \
                -storepass "${PKCS12_PASSWORD}"

        keytool -importcert -noprompt \
                -alias "${name}" \
                -file "${CERTS_DIR:?}/${name}.pem" \
                -keystore "${p12}" \
                -storepass "${PKCS12_PASSWORD}" \
                -keypass "${PKCS12_PASSWORD}"
}

generate_ca
generate_truststore
generate_identity admin "DNS:kafka1,DNS:kafka2,DNS:kafka3"
generate_identity kafka "DNS:kafka1,DNS:kafka2,DNS:kafka3"
generate_identity kafka-ui "DNS:kafka-ui"
generate_identity schema-registry "DNS:schema-registry"
generate_identity connect "DNS:connect"
generate_identity order-api "DNS:order-api"
generate_identity inventory-service "DNS:inventory-service"

for ext in csr pem; do rm -f "${CERTS_DIR:?}"/*."${ext}"; done
rm -f "${CERTS_DIR:?}/ca.p12"

touch "${CERTS_DIR:?}/.generated"

# for kafka admin cli, FIXME: envsubst
cat >"${CERTS_DIR:?}/client.properties" <<EOF
security.protocol=SSL
ssl.key.password=${PKCS12_PASSWORD}
ssl.keystore.location=${SECRETS_DIR}/admin.p12
ssl.keystore.password=${PKCS12_PASSWORD}
ssl.keystore.type=PKCS12
ssl.truststore.location=${SECRETS_DIR}/truststore.p12
ssl.truststore.password=${PKCS12_PASSWORD}
ssl.truststore.type=PKCS12
EOF

chmod 0644 "${CERTS_DIR:?}"/*
