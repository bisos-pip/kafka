#!/usr/bin/env bash
# Real Kerberos handshake test for Kafka client libraries, entirely local and throwaway.
#   - a private MIT KDC on loopback, realm KAFKATEST.LOCAL (invented; nothing to do with any real realm)
#   - a second, private Kafka node (your bisos.kafka broker is not touched) with ONE listener,
#     SASL_SSL + GSSAPI, a self-signed certificate, the broker's own keytab
#   - clients: each "ck:<python>" or "kp:<python>" argument is one run (confluent-kafka / kafka-python)
#   - negative runs (must fail): keytab of a deleted principal, wrong service name
# Needs: krb5-kdc krb5-admin-server krb5-user openssl keytool and /opt/kafka/current (java), run as a normal user.
# Usage: kerberosHandshakeTest.sh ck:/path/venv/bin/python [kp:/path/venv/bin/python ...]    KEEP=1 keeps the workdir.
set -u
REALM=KAFKATEST.LOCAL; HOST=${HOST:-127.0.0.1}   # an IP literal: a dotless name such as "localhost" can be expanded with the host's DNS search domains
KDCP=${KDCP:-26088}; BROKERP=${BROKERP:-26107}; CTRLP=${CTRLP:-26108}; INTP=${INTP:-26109}
KAFKA=${KAFKA:-/opt/kafka/current}
HERE=$(cd "$(dirname "$0")" && pwd)
W=${WORK:-$(mktemp -d)}; mkdir -p "$W"/{krb,tls,data,logs}
export KRB5_CONFIG=$W/krb/krb5.conf KRB5_KDC_PROFILE=$W/krb/kdc.conf
fails=0

cleanup() {
  [ -f "$W/broker.pid" ] && kill "$(cat "$W/broker.pid")" 2>/dev/null
  [ -f "$W/krb/kdc.pid" ] && kill "$(cat "$W/krb/kdc.pid")" 2>/dev/null
  sleep 2; [ "${KEEP:-0}" = 1 ] && echo "workdir kept: $W" || rm -rf "$W"
}
trap cleanup EXIT

cat >"$W/krb/krb5.conf" <<EOT
[libdefaults]
  default_realm = $REALM
  dns_lookup_kdc = false
  dns_lookup_realm = false
  rdns = false
  dns_canonicalize_hostname = false
  udp_preference_limit = 1
[realms]
  $REALM = {
    kdc = 127.0.0.1:$KDCP
  }
EOT
cat >"$W/krb/kdc.conf" <<EOT
[kdcdefaults]
  kdc_listen = 127.0.0.1:$KDCP
  kdc_tcp_listen = 127.0.0.1:$KDCP
[realms]
  $REALM = {
    database_name = $W/krb/principal
    key_stash_file = $W/krb/.k5
    acl_file = $W/krb/kadm5.acl
    max_life = 1h
  }
EOT
: >"$W/krb/kadm5.acl"
kdb5_util create -s -r $REALM -P masterpw >"$W/krb/create.log" 2>&1 || { echo "kdb5_util create failed"; tail -3 "$W/krb/create.log"; exit 2; }
krb5kdc -n -P "$W/krb/kdc.pid" >"$W/krb/kdc.log" 2>&1 &
for i in $(seq 20); do (echo >/dev/tcp/127.0.0.1/$KDCP) 2>/dev/null && break; sleep 0.5; done
kq() { kadmin.local -q "$1" >/dev/null 2>&1; }
kq "addprinc -randkey kafka/$HOST@$REALM";  kq "ktadd -k $W/krb/broker.keytab kafka/$HOST@$REALM"
kq "addprinc -randkey svc_test@$REALM";     kq "ktadd -k $W/krb/svc_test.keytab svc_test@$REALM"
kq "addprinc -randkey svc_gone@$REALM";     kq "ktadd -k $W/krb/svc_gone.keytab svc_gone@$REALM"; kq "delprinc -force svc_gone@$REALM"
[ -s "$W/krb/broker.keytab" ] && [ -s "$W/krb/svc_test.keytab" ] || { echo "keytab creation failed"; exit 2; }
echo "KDC up (127.0.0.1:$KDCP, realm $REALM); principals: kafka/$HOST, svc_test, svc_gone (deleted)"

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$W/tls/key.pem" -out "$W/tls/cert.pem" -days 2 \
  -subj "/CN=$HOST" -addext "subjectAltName=DNS:$HOST,IP:127.0.0.1" 2>/dev/null
openssl pkcs12 -export -in "$W/tls/cert.pem" -inkey "$W/tls/key.pem" -out "$W/tls/keystore.p12" \
  -passout pass:changeit -name broker 2>/dev/null

cat >"$W/server.properties" <<EOT
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:$CTRLP
listeners=SASL_SSL://$HOST:$BROKERP,INTERNAL://127.0.0.1:$INTP,CONTROLLER://127.0.0.1:$CTRLP
advertised.listeners=SASL_SSL://$HOST:$BROKERP,INTERNAL://127.0.0.1:$INTP
controller.listener.names=CONTROLLER
inter.broker.listener.name=INTERNAL
listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,SASL_SSL:SASL_SSL
sasl.enabled.mechanisms=GSSAPI
sasl.kerberos.service.name=kafka
listener.name.sasl_ssl.gssapi.sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab="$W/krb/broker.keytab" principal="kafka/$HOST@$REALM";
ssl.keystore.type=PKCS12
ssl.keystore.location=$W/tls/keystore.p12
ssl.keystore.password=changeit
ssl.key.password=changeit
log.dirs=$W/data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
EOT
export LOG_DIR=$W/logs KAFKA_HEAP_OPTS="-Xms256m -Xmx512m" KAFKA_OPTS="-Djava.security.krb5.conf=$W/krb/krb5.conf"
"$KAFKA/bin/kafka-storage.sh" format --standalone -t "$("$KAFKA/bin/kafka-storage.sh" random-uuid)" \
  -c "$W/server.properties" >"$W/format.log" 2>&1 || { echo "storage format failed"; tail -5 "$W/format.log"; exit 2; }
"$KAFKA/bin/kafka-server-start.sh" "$W/server.properties" >"$W/broker.log" 2>&1 &
echo $! >"$W/broker.pid"
for i in $(seq 60); do (echo >/dev/tcp/127.0.0.1/$BROKERP) 2>/dev/null && break; sleep 1; done
sleep 5
echo "Kafka up (SASL_SSL+GSSAPI on $HOST:$BROKERP, private node; log: $W/broker.log)"; echo

run() {  # label expect(ok|fail) lib python keytab principal service
  local label=$1 expect=$2 lib=$3 py=$4 kt=$5 pr=$6 svc=$7
  echo "== $label"
  export KRB5CCNAME=FILE:$W/cc_$$_$RANDOM
  out=$("$py" "$HERE/kerberosClient.py" "$lib" "$HOST:$BROKERP" "$W/tls/cert.pem" "$svc" "$kt" "$pr" 1000 2>&1); rc=$?
  echo "$out" | sed -E 's/^%[0-9]\|[0-9.]+\|//' | awk '!seen[$0]++' | head -${LINES_MAX:-12} | sed 's/^/   /' | cut -c1-420
  if { [ $expect = ok ] && [ $rc -eq 0 ]; } || { [ $expect = fail ] && [ $rc -ne 0 ]; }; then echo "   => as expected ($expect)"
  else echo "   => UNEXPECTED (wanted $expect, rc=$rc)"; fails=$((fails+1)); fi
  echo
}
for spec in "$@"; do
  kind=${spec%%:*}; py=${spec#*:}; [ $kind = ck ] && lib=confluent || lib=kafka-python
  run "$lib [$py]: valid keytab, right service name" ok $lib "$py" "$W/krb/svc_test.keytab" svc_test@$REALM kafka
done
for spec in "$@"; do
  kind=${spec%%:*}; py=${spec#*:}
  [ $kind = ck ] || continue
  run "confluent [$py]: NEGATIVE keytab of a deleted principal" fail confluent "$py" "$W/krb/svc_gone.keytab" svc_gone@$REALM kafka
  run "confluent [$py]: NEGATIVE wrong service name" fail confluent "$py" "$W/krb/svc_test.keytab" svc_test@$REALM notkafka
  break
done
[ $fails -eq 0 ] && echo "ALL AS EXPECTED" || echo "$fails UNEXPECTED RESULT(S)"
exit $fails
