#!/bin/bash
# Simple test for Stripe connector binary

set -e

echo "Testing Stripe connector binary..."

# Test 1: Missing API key
echo "Test 1: Missing API key (should fail)"
if echo '{}' | ./target/release/vejas-stripe payment 2>&1 | grep -q "STRIPE_API_KEY required"; then
    echo "✓ Test 1 passed: Correctly requires API key"
else
    echo "✗ Test 1 failed"
    exit 1
fi

# Test 2: Payment mode with dummy key (will fail on API but binary works)
echo "Test 2: Payment mode with dummy credentials"
export STRIPE_API_KEY="sk_test_dummy"
if echo '{"amount": 1000, "currency": "usd"}' | ./target/release/vejas-stripe payment 2>&1 | grep -q "Stripe API error"; then
    echo "✓ Test 2 passed: Binary processes input and makes API call"
else
    echo "✗ Test 2 failed"
    exit 1
fi

# Test 3: JSON output format
echo "Test 3: JSON output format"
OUTPUT=$(echo '{"amount": 1000, "currency": "usd"}' | STRIPE_API_KEY="sk_test_dummy" ./target/release/vejas-stripe payment 2>&1 | tail -1)
if echo "$OUTPUT" | jq -e '.ok != null and .processed != null' > /dev/null 2>&1; then
    echo "✓ Test 3 passed: Valid JSON output"
else
    echo "✗ Test 3 failed: Invalid JSON output"
    exit 1
fi

echo ""
echo "✅ All tests passed for Stripe connector"
