#!/bin/bash
# Integration test for refactored connectors with Vejas runtime

set -e

echo "=== Testing Refactored Connectors with Vejas Runtime ==="
echo ""

# Setup test environment
export VEJAS_SECRET_STRIPE_API_KEY="sk_test_dummy"
export VEJAS_SECRET_GITHUB_TOKEN="ghp_dummy"
export VEJAS_SECRET_GITHUB_REPO="owner/repo"
export VEJAS_SECRET_TWILIO_ACCOUNT_SID="AC123"
export VEJAS_SECRET_TWILIO_AUTH_TOKEN="dummy"
export VEJAS_SECRET_TWILIO_FROM_NUMBER="+15555555555"
export VEJAS_SECRET_JIRA_EMAIL="test@example.com"
export VEJAS_SECRET_JIRA_API_TOKEN="dummy"

# Create minimal test directory
TEST_DIR=$(mktemp -d)
echo "Test directory: $TEST_DIR"

# Copy only the new connector manifests
cp connectors/stripe_events_poll.vjs "$TEST_DIR/"
cp connectors/stripe_create_payment.vjs "$TEST_DIR/"
cp connectors/github_issues_poll.vjs "$TEST_DIR/"
cp connectors/twilio_send_sms.vjs "$TEST_DIR/"
cp connectors/jira_issues_poll.vjs "$TEST_DIR/"

# Update manifests to use absolute paths to binaries
VEJAS_ROOT=$(pwd)
for manifest in "$TEST_DIR"/*.vjs; do
    sed -i "s|./connectors/|$VEJAS_ROOT/connectors/|g" "$manifest"
done

echo ""
echo "Test 1: Verify runtime can load exec-stream-source connectors"
echo "--------------------------------------------------------------"

# Start runtime in background with test directory
cd "$TEST_DIR"
timeout 5 "$VEJAS_ROOT/core/target/release/vejas-runtime" 2>&1 | tee runtime.log &
RUNTIME_PID=$!

sleep 2

# Check if runtime loaded the connectors
if grep -q "exec-stream-source" runtime.log; then
    echo "✓ Runtime loaded exec-stream-source connectors"
else
    echo "✗ Runtime did not load exec-stream-source connectors"
    kill $RUNTIME_PID 2>/dev/null || true
    exit 1
fi

if grep -q "exec-sink" runtime.log; then
    echo "✓ Runtime loaded exec-sink connectors"
else
    echo "✗ Runtime did not load exec-sink connectors"
    kill $RUNTIME_PID 2>/dev/null || true
    exit 1
fi

# Kill runtime
kill $RUNTIME_PID 2>/dev/null || true
wait $RUNTIME_PID 2>/dev/null || true

echo ""
echo "Test 2: Verify connector binaries are executable"
echo "------------------------------------------------"

for binary in stripe github twilio jira; do
    BINARY_PATH="$VEJAS_ROOT/connectors/$binary/target/release/vejas-$binary"
    if [ -x "$BINARY_PATH" ]; then
        echo "✓ $binary binary is executable"
    else
        echo "✗ $binary binary is not executable"
        exit 1
    fi
done

echo ""
echo "Test 3: Verify manifest syntax"
echo "-------------------------------"

for manifest in "$TEST_DIR"/*.vjs; do
    if grep -q "driver \"exec-" "$manifest"; then
        echo "✓ $(basename $manifest) has correct driver"
    else
        echo "✗ $(basename $manifest) has incorrect driver"
        exit 1
    fi
done

# Cleanup
rm -rf "$TEST_DIR"

echo ""
echo "=== ✅ All Integration Tests Passed ==="
echo ""
echo "Summary:"
echo "- Runtime successfully loads exec-stream-source connectors"
echo "- Runtime successfully loads exec-sink connectors"
echo "- All 4 connector binaries are executable"
echo "- All 5 manifests have correct syntax"
echo ""
echo "Next steps:"
echo "1. Test with real API credentials"
echo "2. Update admission tests in docs/examples/connectors/"
echo "3. Commit changes to branches"
echo "4. Create pull requests"
