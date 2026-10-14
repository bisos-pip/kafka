import time, sys
from kafka import KafkaProducer, KafkaConsumer
BS='127.0.0.1:24007'; N=int(sys.argv[1]); T='tx-kp'
line=b'x'*120
p=KafkaProducer(bootstrap_servers=BS, transactional_id='kp-1', linger_ms=20, batch_size=262144, acks='all')
p.init_transactions()
# aborted transaction
p.begin_transaction()
for i in range(1000): p.send(T, key=b'k', value=b'ABORTED')
p.abort_transaction()
# committed transaction
t=time.time()
p.begin_transaction()
for i in range(N): p.send(T, key=b'k', value=line, headers=[('seq', str(i).encode())])
p.send(T, key=b'k', value=b'END')
p.commit_transaction()
dt=time.time()-t
print(f'kafka-python produce+commit {N} rec: {dt:.2f}s = {N/dt:,.0f} rec/s')
c=KafkaConsumer(T, bootstrap_servers=BS, group_id=None, auto_offset_reset='earliest', isolation_level='read_committed', consumer_timeout_ms=8000)
n=0; ab=0; t=time.time()
for m in c:
    if m.value==b'ABORTED': ab+=1
    n+=1
    if m.value==b'END': break
dt=time.time()-t
print(f'kafka-python read_committed saw {n} records, aborted visible={ab}, {n/dt:,.0f} rec/s')
