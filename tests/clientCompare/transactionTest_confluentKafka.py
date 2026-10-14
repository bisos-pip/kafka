import time, sys
from confluent_kafka import Producer, Consumer
BS='127.0.0.1:24007'; N=int(sys.argv[1]); T='tx-ck'
line=b'x'*120
p=Producer({'bootstrap.servers':BS,'transactional.id':'ck-1','linger.ms':20,'batch.size':262144,'queue.buffering.max.messages':1000000})
p.init_transactions()
p.begin_transaction()
for i in range(1000): p.produce(T,key=b'k',value=b'ABORTED')
p.abort_transaction()
t=time.time()
p.begin_transaction()
for i in range(N):
    while True:
        try: p.produce(T,key=b'k',value=line,headers=[('seq',str(i).encode())]); break
        except BufferError: p.poll(0.05)
p.produce(T,key=b'k',value=b'END')
p.commit_transaction()
dt=time.time()-t
print(f'confluent-kafka produce+commit {N} rec: {dt:.2f}s = {N/dt:,.0f} rec/s')
c=Consumer({'bootstrap.servers':BS,'group.id':'ck-g','auto.offset.reset':'earliest','isolation.level':'read_committed','enable.auto.commit':False})
c.subscribe([T]); n=0; ab=0; t=time.time(); end=False; idle=time.time()
while not end and time.time()-idle<20:
    m=c.poll(1.0)
    if m is None or m.error(): continue
    idle=time.time(); n+=1
    if m.value()==b'ABORTED': ab+=1
    if m.value()==b'END': end=True
dt=time.time()-t
print(f'confluent-kafka read_committed saw {n} records, aborted visible={ab}, {n/dt:,.0f} rec/s')
c.close()
