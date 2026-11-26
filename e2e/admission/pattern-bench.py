#!/usr/bin/env python3
"""The credential-pattern benchmark (ADR-0017).

98 labeled key names: 55 real keys harvested from this repo, agent-generated
manifests and a production deployment, plus common env-var conventions; and
43 camelCase names as JSON APIs spell them (OAuth and OIDC, Google, Microsoft,
AWS, Slack). Run against the single-sourced pattern (`vejas-runtime
secret-pattern`) to measure its profile, each key read as `secrets::key_as_read`
reads it (camelCase as snake_case); the assertions pin the decided trade-off so
a pattern change re-opens the discussion with numbers, not opinions.

    e2e/admission/pattern-bench.py [pattern]   (default: ask the runtime)
"""
import json, os, re, subprocess, sys

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
    # camelCase credentials, as JSON APIs name them
    'accessToken': SECRET, 'refreshToken': SECRET, 'idToken': SECRET, 'authToken': SECRET,
    'sessionToken': SECRET, 'SessionToken': SECRET, 'bearerToken': SECRET, 'apiToken': SECRET,
    'securityToken': SECRET, 'oauthToken': SECRET, 'personalAccessToken': SECRET, 'botToken': SECRET,
    'appToken': SECRET, 'verificationToken': SECRET, 'csrfToken': SECRET,
    'apiKey': SECRET, 'clientSecret': SECRET, 'password': SECRET, 'SecretAccessKey': SECRET,
    'privateKey': SECRET, 'accessKey': SECRET, 'masterKey': SECRET,
    # camelCase plain names that sit close
    'nextPageToken': PLAIN, 'pageToken': PLAIN, 'continuationToken': PLAIN, 'syncToken': PLAIN,
    'NextToken': PLAIN, 'maxTokens': PLAIN, 'tokenType': PLAIN, 'tokenUrl': PLAIN,
    'tokenEndpoint': PLAIN, 'expiresIn': PLAIN, 'author': PLAIN, 'authorName': PLAIN,
    'authType': PLAIN, 'authMethod': PLAIN, 'publicKey': PLAIN, 'keyId': PLAIN, 'AccessKeyId': PLAIN,
    'primaryKey': PLAIN, 'deviceToken': PLAIN, 'registrationToken': PLAIN, 'passengerCount': PLAIN,
}

def as_read(key):
    """A key as secrets::key_as_read reads it: an underscore where a lowercase
    letter or a digit meets an uppercase one."""
    return re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '_', key)

# the floor the decided pattern (P4, 2026-08-22) accepts, by name, and the
# camelCase reading decided on 2026-11-26: 18 camelCase credentials missed
# before it, 3 after, the same floor as PRIVATE_KEY; 9 false masks, the
# pagination tokens and auth types that snake_case names already gave
ACCEPTED_MISSES = {
    'PRIVATE_KEY', 'SIGNING_KEY', 'ENCRYPTION_KEY', 'PASSPHRASE',
    'CREDENTIALS', 'BEARER', 'DATABASE_URL', 'SERVICE_ACCOUNT_JSON',
    'privateKey', 'accessKey', 'masterKey',
}
ACCEPTED_FALSE_MASKS = {
    'AUTH_ENDPOINT',
    'nextPageToken', 'pageToken', 'continuationToken', 'syncToken', 'NextToken',
    'deviceToken', 'registrationToken', 'authType', 'authMethod',
}

def panel_verdicts(pat, names):
    """The panel's own isSecretKey (core/src/panel.html), run by node on each
    name, so that the JS reading cannot drift from this one. None without node."""
    html = open('core/src/panel.html', encoding='utf-8').read()
    lines = [l for l in html.splitlines() if l.startswith(('const keyAsRead ', 'const isSecretKey '))]
    if len(lines) != 2:
        sys.exit("core/src/panel.html: keyAsRead and isSecretKey not found as one-line consts")
    js = (f"const SECRET_KEY_PATTERN = {json.dumps(pat)};\n" + "\n".join(lines)
          + "\nconst names = JSON.parse(require('fs').readFileSync(0, 'utf8'));"
          + "\nprocess.stdout.write(JSON.stringify(names.map(isSecretKey)));")
    try:
        run = subprocess.run(['node', '-e', js], input=json.dumps(names),
                             capture_output=True, text=True, timeout=20)
    except FileNotFoundError:
        return None
    if run.returncode != 0:
        sys.exit(f"node could not run the panel's isSecretKey: {run.stderr.strip()}")
    return dict(zip(names, json.loads(run.stdout)))

def main():
    if len(sys.argv) > 1:
        pat = sys.argv[1]
    else:
        pat = subprocess.run(
            ['core/target/release/vejas-runtime', 'secret-pattern'],
            capture_output=True, text=True, timeout=5).stdout.strip()
    fn = {k for k, l in BENCH.items() if l == SECRET and not re.search(pat, as_read(k), re.I)}
    fp = {k for k, l in BENCH.items() if l == PLAIN and re.search(pat, as_read(k), re.I)}
    print(f"pattern: {pat}")
    print(f"missed credentials ({len(fn)}): {', '.join(sorted(fn)) or '—'}")
    print(f"false masks        ({len(fp)}): {', '.join(sorted(fp)) or '—'}")
    panel = panel_verdicts(pat, list(BENCH))
    if panel is None:
        print("node not found: the panel's reading is not checked")
        if os.environ.get('CI'):
            return 1
    else:
        drift = sorted(k for k in BENCH if panel[k] != bool(re.search(pat, as_read(k), re.I)))
        if drift:
            print(f"\n✗ the panel reads these keys otherwise: {', '.join(drift)}")
            return 1
        print(f"panel (node)       : the same verdict on all {len(BENCH)} names")
    if fn != ACCEPTED_MISSES or fp != ACCEPTED_FALSE_MASKS:
        print("\n✗ profile drifted from the ADR-0017 decision — "
              "either fix the pattern or re-decide (update ADR + this file).")
        return 1
    print("\n✓ profile matches the ADR-0017 decision")
    return 0

if __name__ == '__main__':
    sys.exit(main())
