#!/bin/bash
# Run all Python examples against a live payload-manager service.
# Env vars:
#   PAYLOAD_MANAGER_ENDPOINT  service address (default: localhost:50051)

ENDPOINT="${PAYLOAD_MANAGER_ENDPOINT:-localhost:50051}"
PYTHONPATH="/app/client/python"
EXAMPLES="/app/examples/python"
FAILED=0

run() {
    local name="$1"
    shift
    echo ""
    echo "=========================================="
    echo " Running: $name"
    echo "=========================================="
    if PYTHONPATH="$PYTHONPATH" python3 "$EXAMPLES/$name" "$@"; then
        echo "[PASS] $name"
    else
        echo "[FAIL] $name"
        FAILED=1
    fi
}

run stats_example.py         "$ENDPOINT"
run round_trip_example.py    "$ENDPOINT"
run metadata_example.py      "$ENDPOINT"
run catalog_admin_example.py "$ENDPOINT"
run stream_example.py        "$ENDPOINT"
run list_example.py          "$ENDPOINT" "all"

# Spill example needs a pre-existing payload UUID; use round_trip output
# The round_trip example prints the UUID on its last line as "UUID: <uuid>".
echo ""
echo "=========================================="
echo " Running: spill_example.py (allocate first)"
echo "=========================================="
UUID=$(PYTHONPATH="$PYTHONPATH" python3 "$EXAMPLES/round_trip_example.py" "$ENDPOINT" 2>/dev/null | grep -oP 'UUID=\K[0-9a-f-]+')
if [ -n "$UUID" ]; then
    echo "Using payload UUID: $UUID"
    run spill_example.py "$UUID" "$ENDPOINT"
else
    echo "[SKIP] spill_example.py — could not obtain UUID from round_trip_example"
fi

# GPU example: only run if pyarrow.cuda is available
if python3 -c "import pyarrow.cuda" 2>/dev/null; then
    run gpu_example.py "$ENDPOINT"
else
    echo ""
    echo "=========================================="
    echo " Skipping: gpu_example.py (pyarrow.cuda not available)"
    echo "=========================================="
fi

echo ""
if [ "$FAILED" -ne 0 ]; then
    echo "ERROR: one or more examples FAILED"
    exit 1
fi
echo "All Python examples PASSED"
exit 0
