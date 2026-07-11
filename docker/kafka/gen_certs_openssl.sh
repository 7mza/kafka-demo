#!/bin/sh
# no bash in alpine
# shellcheck shell=dash
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
        openssl req -x509 \
                -newkey rsa:4096 \
                -passout pass:"${CA_KEY_PASSWORD}" \
                -days "${CA_DAYS}" \
                -subj "/CN=rootCA" \
                -keyout "${CERTS_DIR:?}/rootCA.key" \
                -out "${CERTS_DIR:?}/rootCA.pem"
}

generate_truststore() {
        openssl pkcs12 -export -nokeys \
                -in "${CERTS_DIR:?}/rootCA.pem" \
                -out "${CERTS_DIR:?}/truststore.p12" \
                -name rootCA \
                -passout pass:"${PKCS12_PASSWORD}"
}

generate_identity() {
        local name=$1
        local dns=$2
        local p12="${CERTS_DIR:?}/${name}.p12"
        local extfile="${CERTS_DIR:?}/${name}.ext"
        cat >"${extfile}" <<EOF
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth, serverAuth
subjectAltName = ${dns},DNS:localhost,IP:127.0.0.1
EOF

        openssl req -newkey rsa:4096 -nodes \
                -keyout "${CERTS_DIR:?}/${name}.key" \
                -out "${CERTS_DIR:?}/${name}.csr" \
                -subj "/CN=${name}"

        openssl x509 -req \
                -in "${CERTS_DIR:?}/${name}.csr" \
                -CA "${CERTS_DIR:?}/rootCA.pem" \
                -CAkey "${CERTS_DIR:?}/rootCA.key" \
                -passin pass:"${CA_KEY_PASSWORD}" \
                -CAcreateserial \
                -out "${CERTS_DIR:?}/${name}.pem" \
                -days "${CERT_DAYS}" \
                -extfile "${extfile}"

        openssl pkcs12 -export \
                -inkey "${CERTS_DIR:?}/${name}.key" \
                -in "${CERTS_DIR:?}/${name}.pem" \
                -certfile "${CERTS_DIR:?}/rootCA.pem" \
                -out "${p12}" \
                -name "${name}" \
                -passout pass:"${PKCS12_PASSWORD}"
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

for ext in csr key pem ext srl; do rm -f "${CERTS_DIR:?}"/*."${ext}"; done

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
