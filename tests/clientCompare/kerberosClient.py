#!/usr/bin/env python3
"""One Kerberos-authenticated (SASL_SSL + GSSAPI) transactional round trip, as one client library.

Usage: kerberosClient.py {confluent|kafka-python} BOOTSTRAP CACERT SERVICENAME KEYTAB PRINCIPAL [N]
Environment: KRB5_CONFIG and KRB5CCNAME are set by the caller (kerberosHandshakeTest.sh).
Exit 0: N records were committed and read back under read_committed. Exit 1: failed (reason printed).
"""
import os, subprocess, sys, time

lib, bootstrap, cacert, service, keytab, principal = sys.argv[1:7]
N = int(sys.argv[7]) if len(sys.argv) > 7 else 1000
topic = f'krb-{lib}-{os.getpid()}'
errors = []

def fail(msg):
    print(f'FAIL {lib}: {msg}')
    if errors:
        print('     broker/client errors: ' + ' | '.join(sorted(set(errors)))[:400])
    sys.exit(1)

def confluent():
    from confluent_kafka import Producer, Consumer, KafkaException
    import confluent_kafka
    v, lv = confluent_kafka.version(), confluent_kafka.libversion()   # a tuple in 2.8, a str in 2.16 (version)
    print(f'     confluent-kafka {v if isinstance(v, str) else v[0]}, librdkafka {lv if isinstance(lv, str) else lv[0]}')
    base = {'bootstrap.servers': bootstrap, 'security.protocol': 'SASL_SSL', 'sasl.mechanisms': 'GSSAPI',
            'sasl.kerberos.service.name': service, 'sasl.kerberos.keytab': keytab,
            'sasl.kerberos.principal': principal, 'ssl.ca.location': cacert,
            'error_cb': lambda e: errors.append(str(e))}
    try:
        p = Producer({**base, 'transactional.id': f'krb-{os.getpid()}'})
        p.init_transactions(20)
        p.begin_transaction()
        for i in range(N):
            p.produce(topic, key=b'k', value=f'line {i}'.encode(), headers=[('seq', str(i).encode())])
        p.commit_transaction(20)
    except KafkaException as e:
        fail(f'produce: {e}')
    c = Consumer({**base, 'group.id': f'g-{os.getpid()}', 'auto.offset.reset': 'earliest',
                  'isolation.level': 'read_committed', 'enable.auto.commit': False})
    c.subscribe([topic]); got = 0; idle = time.time()
    while got < N and time.time() - idle < 20:
        m = c.poll(1.0)
        if m is not None and not m.error():
            got += 1; idle = time.time()
    c.close()
    return got

def kafkaPython():
    import kafka
    from kafka import KafkaProducer, KafkaConsumer
    print(f'     kafka-python {kafka.__version__} (+ gssapi); ticket obtained with kinit, as existing scripts commonly do')
    subprocess.run(['kinit', '-kt', keytab, principal], check=True)
    base = dict(bootstrap_servers=bootstrap, security_protocol='SASL_SSL', sasl_mechanism='GSSAPI',
                sasl_kerberos_service_name=service, ssl_cafile=cacert)
    try:
        p = KafkaProducer(**base, transactional_id=f'krb-{os.getpid()}')
        p.init_transactions(); p.begin_transaction()
        for i in range(N):
            p.send(topic, key=b'k', value=f'line {i}'.encode(), headers=[('seq', str(i).encode())])
        p.commit_transaction()
    except Exception as e:
        fail(f'produce: {type(e).__name__}: {e}')
    c = KafkaConsumer(topic, **base, group_id=None, auto_offset_reset='earliest',
                      isolation_level='read_committed', consumer_timeout_ms=15000)
    return sum(1 for _ in c)

try:
    got = confluent() if lib == 'confluent' else kafkaPython()
except SystemExit:
    raise
except Exception as e:
    fail(f'{type(e).__name__}: {e}')
if got != N:
    fail(f'read back {got} of {N} records')
print(f'OK   {lib}: {N} records committed in one transaction and read back (read_committed) over SASL_SSL + GSSAPI')
