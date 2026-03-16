#!/bin/bash
# Run all C++ examples against a live payload-manager service.
# Env vars:
#   PAYLOAD_MANAGER_ENDPOINT  service address (default: localhost:50051)

ENDPOINT="${PAYLOAD_MANAGER_ENDPOINT:-localhost:50051}"
FAILED=0

run() {
    local name="$1"
    shift
    echo ""
    echo "=========================================="
    echo " Running: $name"
    echo "=========================================="
    if "/app/$name" "$@"; then
        echo "[PASS] $name"
    else
        echo "[FAIL] $name"
        FAILED=1
    fi
}

# Standalone examples (no pre-existing payload required)
run payload_manager_example_stats         "$ENDPOINT" ""
run payload_manager_example_round_trip    "$ENDPOINT" ""
run payload_manager_example_metadata      "$ENDPOINT" ""
run payload_manager_example_catalog_admin "$ENDPOINT" ""
run payload_manager_example_stream        "$ENDPOINT" ""
run payload_manager_example_list          "$ENDPOINT" "all" ""

# Allocate a payload, then verify with read and spill
echo ""
echo "=========================================="
echo " Running: payload_manager_example_allocate"
echo "=========================================="
UUID=$(/app/payload_manager_example_allocate "$ENDPOINT" 65536 "")
EXIT=$?
if [ $EXIT -ne 0 ]; then
    echo "[FAIL] payload_manager_example_allocate"
    FAILED=1
else
    echo "[PASS] payload_manager_example_allocate → UUID=$UUID"
    run payload_manager_example_read  "$UUID" "$ENDPOINT" ""
    run payload_manager_example_spill "$UUID" "$ENDPOINT" ""
fi

# GPU example: only present when built with PAYLOAD_MANAGER_CLIENT_ENABLE_CUDA=ON
if [ -f "/app/payload_manager_example_gpu" ]; then
    run payload_manager_example_gpu "$ENDPOINT" ""
fi

echo ""
if [ "$FAILED" -ne 0 ]; then
    echo "ERROR: one or more examples FAILED"
    exit 1
fi
echo "All examples PASSED"
exit 0
