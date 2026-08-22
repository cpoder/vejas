#!/usr/bin/env python3
"""The credential-pattern benchmark (ADR-0017).

55 labeled key names: real keys harvested from this repo, agent-generated
manifests and a production deployment, plus common env-var conventions.
Run against the single-sourced pattern (`vejas-runtime secret-pattern`) to
measure its profile; the assertions pin the decided trade-off so a pattern
change re-opens the discussion with numbers, not opinions.

    e2e/admission/pattern-bench.py [pattern]   (default: ask the runtime)
"""
import re, subprocess, sys

SECRET, PLAIN = 1, 0
BENCH = {
    # real, harvested, secret-bearing
    'CLIENT_SECRET': SECRET, 'WEBHOOK_URL': SECRET, 'SAP_PASSWD': SECRET,
    # real, harvested, plain business/config literals
    'TOKEN_URL': PLAIN, 'JIRA_PROJECT_KEY': PLAIN, 'FRAMEWORK_KEY': PLAIN,
    'PASS_THRESHOLD_PCT': PLAIN, 'API_REQUEST': PLAIN, 'API_RESPONSE': PLAIN,
    'SEVERITY_CODES': PLAIN, 'COUNTRY_CODES': PLAIN, 'INTERVAL_SECS': PLAIN,
    'SUBJECT': PLAIN, 'URL': PLAIN, 'ENDPOINTS': PLAIN, 'SCOPE': PLAIN,
    'PAYLOAD': PLAIN, 'TEST_BODY': PLAIN, 'ENVELOPE': PLAIN, 'CLIENT_ID': PLAIN,
    # well-known credential names
    'AWS_SECRET_ACCESS_KEY': SECRET, 'GITHUB_TOKEN': SECRET,
    'SLACK_BOT_TOKEN': SECRET, 'DATABASE_PASSWORD': SECRET,
    'SMTP_PASSWORD': SECRET, 'API_KEY': SECRET, 'APIKEY': SECRET,
    'API_SECRET': SECRET, 'ACCESS_TOKEN': SECRET, 'REFRESH_TOKEN': SECRET,
    'AUTH_TOKEN': SECRET, 'BASIC_AUTH': SECRET, 'AUTHORIZATION': SECRET,
    'AUTH': SECRET, 'CONSUMER_SECRET': SECRET,
    # the documented irreducible floor (no name pattern gets these sanely)
    'PRIVATE_KEY': SECRET, 'SIGNING_KEY': SECRET, 'ENCRYPTION_KEY': SECRET,
    'PASSPHRASE': SECRET, 'CREDENTIALS': SECRET, 'BEARER': SECRET,
    'DATABASE_URL': SECRET, 'SERVICE_ACCOUNT_JSON': SECRET,
    # well-known plain names that flirt with the pattern
    'AUTHOR': PLAIN, 'AUTHORS': PLAIN, 'AUTH_ENDPOINT': PLAIN,
    'OAUTH_URL': PLAIN, 'OAUTH_SCOPE': PLAIN, 'TOKEN_TTL_SECS': PLAIN,
    'MAX_TOKENS': PLAIN, 'KEYWORDS': PLAIN, 'KEYBOARD_LAYOUT': PLAIN,
    'WORD_COUNT': PLAIN, 'PASSENGER_COUNT': PLAIN, 'WEBHOOK_PATH': PLAIN,
}

# the floor the decided pattern (P4, 2026-08-22) accepts, by name
ACCEPTED_MISSES = {
    'PRIVATE_KEY', 'SIGNING_KEY', 'ENCRYPTION_KEY', 'PASSPHRASE',
    'CREDENTIALS', 'BEARER', 'DATABASE_URL', 'SERVICE_ACCOUNT_JSON',
}
ACCEPTED_FALSE_MASKS = {'AUTH_ENDPOINT'}

def main():
    if len(sys.argv) > 1:
        pat = sys.argv[1]
    else:
        pat = subprocess.run(
            ['core/target/release/vejas-runtime', 'secret-pattern'],
            capture_output=True, text=True, timeout=5).stdout.strip()
    fn = {k for k, l in BENCH.items() if l == SECRET and not re.search(pat, k, re.I)}
    fp = {k for k, l in BENCH.items() if l == PLAIN and re.search(pat, k, re.I)}
    print(f"pattern: {pat}")
    print(f"missed credentials ({len(fn)}): {', '.join(sorted(fn)) or '—'}")
    print(f"false masks        ({len(fp)}): {', '.join(sorted(fp)) or '—'}")
    if fn != ACCEPTED_MISSES or fp != ACCEPTED_FALSE_MASKS:
        print("\n✗ profile drifted from the ADR-0017 decision — "
              "either fix the pattern or re-decide (update ADR + this file).")
        return 1
    print("\n✓ profile matches the ADR-0017 decision")
    return 0

if __name__ == '__main__':
    sys.exit(main())
