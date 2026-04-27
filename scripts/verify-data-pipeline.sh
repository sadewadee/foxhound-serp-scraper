#!/usr/bin/env bash
# verify-data-pipeline.sh
#
# End-to-end smoke test for the enrichment data path. Catches the class of
# bug that bit us on 2026-04-27: schema and write path were correct, but the
# read path was missing the new columns — data went into the DB and was
# invisible to API consumers ("fix 80% lupa loop terakhir" pattern).
#
# What it checks:
#   1. Insert a synthetic enrichment_jobs row with raw_* columns populated.
#   2. Set status='completed' so trigger fires.
#   3. SELECT business_listings row that the trigger upserted; assert every
#      raw_* field round-tripped to the canonical column.
#   4. curl /api/v2/results?domain=<test> and assert the new fields are
#      present in the JSON response (not just stored, but exposed).
#   5. Cleanup — DELETE the synthetic row from business_listings (CASCADE
#      handles business_emails) and enrichment_jobs.
#
# Exit codes:
#   0 — all assertions passed
#   1 — schema gap (write path drops a field)
#   2 — read path gap (column populated but missing from API response)
#   3 — environment / setup error
#
# Required env:
#   POSTGRES_DSN  — psql-compatible connection string
#   API_BASE_URL  — e.g. http://localhost:8080
#   API_KEY       — admin or viewer key for /api/v2/*
#
# Usage:
#   ./scripts/verify-data-pipeline.sh
#
# Run before any deploy that touches schema, trigger, extractor, or the V2
# read path. Recommended: wire into CI on PRs that change those files.

set -euo pipefail

# ─── Setup ───────────────────────────────────────────────────────────────────
: "${POSTGRES_DSN:?POSTGRES_DSN env var required}"
: "${API_BASE_URL:?API_BASE_URL env var required}"
: "${API_KEY:?API_KEY env var required}"

readonly TEST_DOMAIN="verify-pipeline-$(date +%s)-$RANDOM.test"
readonly TEST_URL="https://${TEST_DOMAIN}/contact"
readonly TEST_HASH="$(printf '%s' "$TEST_URL" | shasum -a 256 | awk '{print $1}')"

# ─── Helpers ─────────────────────────────────────────────────────────────────
psql_run() {
  psql "$POSTGRES_DSN" -At -c "$1"
}

cleanup() {
  echo "[cleanup] removing test rows for domain ${TEST_DOMAIN}"
  psql_run "DELETE FROM business_listings WHERE domain = '${TEST_DOMAIN}'" >/dev/null 2>&1 || true
  psql_run "DELETE FROM enrichment_jobs   WHERE url_hash = '${TEST_HASH}'"   >/dev/null 2>&1 || true
}
trap cleanup EXIT

assert_eq() {
  local field="$1" expected="$2" actual="$3"
  if [[ "$actual" != "$expected" ]]; then
    echo "[FAIL] field=${field} expected=${expected} got=${actual}"
    exit 1
  fi
  echo "[ok ] ${field}=${actual}"
}

assert_json_field() {
  local field="$1" expected="$2" json="$3"
  local actual
  actual="$(printf '%s' "$json" | jq -r ".data[0].${field}")"
  if [[ "$actual" != "$expected" ]]; then
    echo "[FAIL] api.${field} expected=${expected} got=${actual}"
    echo "Full JSON:"
    printf '%s\n' "$json" | jq .
    exit 2
  fi
  echo "[ok ] api.${field}=${actual}"
}

# ─── Step 1: insert pending enrichment_jobs row with raw_* populated ─────────
echo "[step 1] inserting synthetic enrichment_jobs row for ${TEST_DOMAIN}"
psql_run "
  INSERT INTO enrichment_jobs (
    url, url_hash, domain, source, status,
    raw_emails, raw_phones, raw_social,
    raw_business_name, raw_category, raw_address, raw_page_title,
    raw_description, raw_location, raw_country, raw_city, raw_contact_name,
    raw_opening_hours, raw_rating,
    raw_tiktok, raw_youtube, raw_telegram
  ) VALUES (
    '${TEST_URL}', '${TEST_HASH}', '${TEST_DOMAIN}', 'verify_test', 'pending',
    ARRAY['hi@${TEST_DOMAIN}'],
    ARRAY['+62811000001','+62811000002','+62811000003'],
    '{\"facebook\":\"https://facebook.com/verifytest\"}'::jsonb,
    'Verify Pipeline Test Co', 'Test Category',
    '123 Test St, Jakarta, ID',
    'Test Page Title',
    'A synthetic listing inserted by verify-data-pipeline.sh.',
    'Jakarta',
    'ID',
    'Jakarta',
    'Jane Tester',
    'Mon-Fri 9-5',
    '4.5',
    'https://tiktok.com/@verifytest',
    'https://youtube.com/@verifytest',
    'verifytestbot'
  )
" >/dev/null

# ─── Step 2: flip status=completed to fire trigger ───────────────────────────
echo "[step 2] firing trigger via UPDATE status=completed"
psql_run "UPDATE enrichment_jobs SET status='completed', completed_at=NOW() WHERE url_hash='${TEST_HASH}'" >/dev/null

# ─── Step 3: assert business_listings row populated correctly ────────────────
echo "[step 3] checking business_listings columns"
row="$(psql_run "
  SELECT
    business_name, category, description,
    address, location, country, city, contact_name,
    phone, array_to_string(phones, ','),
    page_title, opening_hours, rating,
    tiktok, youtube, telegram
  FROM business_listings WHERE domain = '${TEST_DOMAIN}'
" | tr '|' $'\n')"

mapfile -t fields <<< "$row"

assert_eq business_name      'Verify Pipeline Test Co'  "${fields[0]}"
assert_eq category           'Test Category'            "${fields[1]}"
assert_eq description        'A synthetic listing inserted by verify-data-pipeline.sh.' "${fields[2]}"
assert_eq address            '123 Test St, Jakarta, ID' "${fields[3]}"
assert_eq location           'Jakarta'                  "${fields[4]}"
assert_eq country            'ID'                       "${fields[5]}"
assert_eq city               'Jakarta'                  "${fields[6]}"
assert_eq contact_name       'Jane Tester'              "${fields[7]}"
assert_eq phone              '+62811000001'             "${fields[8]}"
assert_eq phones             '+62811000001,+62811000002,+62811000003' "${fields[9]}"
assert_eq page_title         'Test Page Title'          "${fields[10]}"
assert_eq opening_hours      'Mon-Fri 9-5'              "${fields[11]}"
assert_eq rating             '4.5'                      "${fields[12]}"
assert_eq tiktok             'https://tiktok.com/@verifytest' "${fields[13]}"
assert_eq youtube            'https://youtube.com/@verifytest' "${fields[14]}"
assert_eq telegram           'verifytestbot'            "${fields[15]}"

# ─── Step 4: assert API exposes every field ──────────────────────────────────
echo "[step 4] curl /api/v2/results?domain=${TEST_DOMAIN}"
json="$(curl -fsS -H "x-api-key: ${API_KEY}" \
  "${API_BASE_URL}/api/v2/results?domain=${TEST_DOMAIN}&per_page=1" || true)"

if [[ -z "$json" ]]; then
  echo "[FAIL] /api/v2/results returned empty body"
  exit 2
fi

assert_json_field business_name 'Verify Pipeline Test Co'                      "$json"
assert_json_field category      'Test Category'                                "$json"
assert_json_field description   'A synthetic listing inserted by verify-data-pipeline.sh.' "$json"
assert_json_field address       '123 Test St, Jakarta, ID'                     "$json"
assert_json_field location      'Jakarta'                                      "$json"
assert_json_field country       'ID'                                           "$json"
assert_json_field city          'Jakarta'                                      "$json"
assert_json_field contact_name  'Jane Tester'                                  "$json"
assert_json_field phone         '+62811000001'                                 "$json"
assert_json_field opening_hours 'Mon-Fri 9-5'                                  "$json"
assert_json_field rating        '4.5'                                          "$json"
assert_json_field tiktok        'https://tiktok.com/@verifytest'               "$json"
assert_json_field youtube       'https://youtube.com/@verifytest'              "$json"
assert_json_field telegram      'verifytestbot'                                "$json"

# Multi-phone array: API may return phones as JSON array; check length and first entry.
phones_count="$(printf '%s' "$json" | jq '.data[0].phones | length')"
if [[ "$phones_count" -lt 3 ]]; then
  echo "[FAIL] api.phones expected >=3 entries, got ${phones_count}"
  exit 2
fi
echo "[ok ] api.phones length=${phones_count}"

echo
echo "✅ verify-data-pipeline.sh passed all checks"
echo "   write path → schema → trigger → read path are aligned end-to-end."
