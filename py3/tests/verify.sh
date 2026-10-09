#!/bin/bash -i
#

# Safety check: must run from a directory named "tests"
if [[ "$(basename "$PWD")" != "tests" ]]; then
    echo "ERROR: verify.sh must be run from a directory named 'tests'." >&2
    echo "  Current PWD: $PWD" >&2
    exit 1
fi

# Starting-point smoke test for bisos.airflow -- covers airflowAdmin.cs (primary
# target for this starting point) and quickAirflow.cs. Converted from
# bisos.dockerProc's py3/tests/verify.sh; will be expanded as the package develops.

# --- CS entry points: smoke test (no args → examples/usage, non-zero ok) ---

lpDo ../bin/airflowAdmin.cs
lpDo ../bin/quickAirflow.cs

# --- airflowAdmin.cs: each admin sub-Cmnd runs and produces examples output ---

lpDo ../bin/airflowAdmin.cs -i examples
lpDo ../bin/airflowAdmin.cs -i hostInfo
lpDo ../bin/airflowAdmin.cs -i servicesInfo
lpDo ../bin/airflowAdmin.cs -i usersInfo
lpDo ../bin/airflowAdmin.cs -i fullUpdate

# --- quickAirflow.cs: default examples entry runs ---

lpDo ../bin/quickAirflow.cs -i examples

# --- airflowAdmin.cs: import + class presence smoke test ---

lpDo python3 -c "
import importlib.util, sys

spec = importlib.util.spec_from_file_location('airflowAdmin', '../bin/airflowAdmin.cs')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

expectedCmnds = [
    'examples', 'fullUpdate', 'hostInfo', 'servicesInfo', 'usersInfo',
]

failed = 0
for n in expectedCmnds:
    if not hasattr(m, n):
        print(f'  [FAIL] missing Cmnd: {n}'); failed += 1
    else:
        print(f'  [PASS] Cmnd present: {n}')

if failed:
    print(f'airflowAdmin.cs: {failed} test(s) FAILED')
    sys.exit(1)
else:
    print('airflowAdmin.cs: all cases PASS')
" 2>/dev/null || true
