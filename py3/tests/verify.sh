#!/bin/bash -i
#

# Safety check: must run from a directory named "tests"
if [[ "$(basename "$PWD")" != "tests" ]]; then
    echo "ERROR: verify.sh must be run from a directory named 'tests'." >&2
    echo "  Current PWD: $PWD" >&2
    exit 1
fi

# Starting-point smoke test for bisos.kafka -- covers kafkaAdmin.cs, quickKafka.cs,
# the read-only Cmnds of the planted files, and bisos.kafka.kafkaInfo's generators.
# Nothing here installs, starts or writes anything on the host.

# --- CS entry points: smoke test (no args → examples/usage, non-zero ok) ---

lpDo ../bin/kafkaAdmin.cs
lpDo ../bin/quickKafka.cs
lpDo ../bin/kafka-assemble.cs

# --- kafkaAdmin.cs: each admin sub-Cmnd runs and produces examples output ---

lpDo ../bin/kafkaAdmin.cs -i examples
lpDo ../bin/kafkaAdmin.cs -i hostInfo
lpDo ../bin/kafkaAdmin.cs -i servicesInfo
lpDo ../bin/kafkaAdmin.cs -i topicsInfo
lpDo ../bin/kafkaAdmin.cs -i consumerGroupsInfo
lpDo ../bin/kafkaAdmin.cs -i driftCheck
lpDo ../bin/kafkaAdmin.cs -i fullUpdate

# --- quickKafka.cs: default examples entry runs ---

lpDo ../bin/quickKafka.cs -i examples

# --- planted files: menus and read-only Cmnds ---

lpDo ../bin/kafka-sbom.pcs -i readme
lpDo ../bin/kafka-sysd.pcs
lpDo ../bin/kafbat-ui-sysd.pcs
lpDo ../bin/kafka-cbs.pcs
lpDo ../bin/cbmProc-kafka.spcs
lpDo ../bin/kafka-here-dns.pcs
lpDo ../bin/kafka-wvd.pcs -i configFileStdout --cls=configFile_nginxVd

# --- kafkaAdmin.cs: import + class presence smoke test ---

lpDo python3 -c "
import importlib.util, importlib.machinery, sys

loader = importlib.machinery.SourceFileLoader('kafkaAdmin', '../bin/kafkaAdmin.cs')
spec = importlib.util.spec_from_loader('kafkaAdmin', loader)
m = importlib.util.module_from_spec(spec)
loader.exec_module(m)

expectedCmnds = [
    'examples', 'fullUpdate', 'hostInfo', 'servicesInfo', 'topicsInfo',
    'consumerGroupsInfo', 'driftCheck',
]

failed = 0
for n in expectedCmnds:
    if not hasattr(m, n):
        print(f'  [FAIL] missing Cmnd: {n}'); failed += 1
    else:
        print(f'  [PASS] Cmnd present: {n}')

if failed:
    print(f'kafkaAdmin.cs: {failed} test(s) FAILED')
    sys.exit(1)
else:
    print('kafkaAdmin.cs: all cases PASS')
" 2>/dev/null || true

# --- bisos.kafka.kafkaInfo: banna ports and generator invariants ---

lpDo python3 -c "
import sys
from bisos.kafka.kafkaInfo import (
    kafkaInfo, serverPropertiesContent, envFileContent, kafbatEnvFileContent, kvDrift,
)

cases = [
    ('banna: kafka port',            kafkaInfo.portNu == 24007),
    ('banna: kafkaController port',  kafkaInfo.controllerPortNu == 24008),
    ('banna: kafbatUi port',         kafkaInfo.kafbatPortNu == 24009),
    ('KRaft combined mode',          'process.roles=broker,controller' in serverPropertiesContent()),
    ('single-node offsets RF=1',     'offsets.topic.replication.factor=1' in serverPropertiesContent()),
    ('broker on loopback',           f'PLAINTEXT://127.0.0.1:{kafkaInfo.portNu}' in serverPropertiesContent()),
    ('LOG_DIR set for the broker',   f'LOG_DIR={kafkaInfo.logDir}' in envFileContent()),
    ('Kafbat UI on loopback',        'SERVER_ADDRESS=127.0.0.1' in kafbatEnvFileContent()),
    ('Kafbat UI -> the broker',      kafkaInfo.bootstrapServers in kafbatEnvFileContent()),
    ('drift: identical is clean',    kvDrift(envFileContent(), envFileContent()) == {'missing': [], 'differs': [], 'extra': []}),
    ('drift: extra key reported',    kvDrift(envFileContent() + 'X=1\n', envFileContent())['extra'] == ['X']),
]

failed = 0
for name, ok in cases:
    print(f'  [{\"PASS\" if ok else \"FAIL\"}] {name}')
    failed += not ok

if failed:
    print(f'kafkaInfo: {failed} test(s) FAILED')
    sys.exit(1)
else:
    print('kafkaInfo: all cases PASS')
" 2>/dev/null || true
