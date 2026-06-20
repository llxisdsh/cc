#!/usr/bin/env bash
set -u

total="${CC_V6_ABA_TOTAL:-30m}"
run="${CC_V6_ABA_RUN:-30s}"
readers="${CC_V6_ABA_READERS:-16}"
pkg="${CC_V6_ABA_PKG:-.}"
test_name='^TestV6MapPublicABAStringCrashHarness$'

if [[ ! "$total" =~ ^[0-9]+[smh]?$ ]]; then
    echo "CC_V6_ABA_TOTAL must look like 30s, 10m, or 1h" >&2
    exit 2
fi

case "$total" in
    *s) total_seconds="${total%s}" ;;
    *m) total_seconds="$(( ${total%m} * 60 ))" ;;
    *h) total_seconds="$(( ${total%h} * 3600 ))" ;;
    *) total_seconds="$total" ;;
esac

start="$(date +%s)"
deadline="$((start + total_seconds))"
attempt=1

echo "V6 public ABA string crash stress"
echo "total=${total} run=${run} readers=${readers} pkg=${pkg}"

while [ "$(date +%s)" -lt "$deadline" ]; do
    now="$(date '+%Y-%m-%d %H:%M:%S')"
    echo "[$now] attempt ${attempt}"
    log="$(mktemp "${TMPDIR:-/tmp}/v6-aba.XXXXXX.log")"

    CC_V6_PUBLIC_ABA_STRING_CRASH=1 \
    CC_V6_PUBLIC_ABA_STRING_CRASH_DURATION="$run" \
    CC_V6_PUBLIC_ABA_STRING_CRASH_READERS="$readers" \
    go test "$pkg" -run "$test_name" -count=1 -v >"$log" 2>&1
    status="$?"

    if [ "$status" -eq 0 ]; then
        echo "reproduced: child process crashed as expected"
        echo "log: $log"
        cat "$log"
        exit 0
    fi

    if grep -q "public ABA crash was not reproduced" "$log"; then
        rm -f "$log"
        attempt="$((attempt + 1))"
        continue
    fi

    echo "unexpected go test failure"
    echo "log: $log"
    cat "$log"
    exit "$status"
done

echo "not reproduced within ${total}"
exit 1
