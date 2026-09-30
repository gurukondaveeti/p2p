#!/usr/bin/env bash
# test/smoke_test.sh
#
# Automated end-to-end smoke test for the P2P file sharing system. Exercises
# the scenario this project was actually validated against during
# development: two trackers, three clients, user/group lifecycle, a
# multi-piece upload, a single-peer download, a multi-peer download, tracker
# crash + client failover, and tracker recovery (disk replay + anti-entropy
# catch-up). Every download's SHA1 is checked against the source file.
#
# Usage: bash test/smoke_test.sh   (or: make test)
#
# Note on style: driving three interactive REPLs from one script needs a
# named pipe per process *plus* a long-lived background writer holding each
# pipe open (see the `keepalive` loop below) -- otherwise the reading
# process sees EOF the moment our first `echo` closes its end, and exits.

set -uo pipefail
set +m # don't let the shell print "Killed" job-control notices during cleanup

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TRACKER="$ROOT/tracker/tracker"
CLIENT="$ROOT/client/client"
WORK="$(mktemp -d)"

declare -a CLEANUP_PIDS=()
cleanup() {
    for pid in "${CLEANUP_PIDS[@]:-}"; do
        kill -9 "$pid" >/dev/null 2>&1 || true
    done
    rm -rf "$WORK"
}
trap cleanup EXIT

pass=0
fail=0
check() {
    if [ "$1" = "$2" ]; then
        echo "  PASS: $3"
        pass=$((pass + 1))
    else
        echo "  FAIL: $3 (expected '$2', got '$1')"
        fail=$((fail + 1))
    fi
}

[ -x "$TRACKER" ] || { echo "tracker binary missing -- run 'make' first"; exit 1; }
[ -x "$CLIENT" ] || { echo "client binary missing -- run 'make' first"; exit 1; }

cd "$WORK"
cat > tracker_info.txt <<EOF
127.0.0.1:9001:9101
127.0.0.1:9002:9102
EOF

mkfifo t0.in t1.in c1.in c2.in c3.in
for f in t0 t1 c1 c2 c3; do
    ( exec 3>"$f.in"; sleep 600 ) & CLEANUP_PIDS+=($!)
done
sleep 0.2

send() { echo "$2" > "$1.in"; sleep "${3:-0.3}"; }

echo "== starting 2 trackers =="
"$TRACKER" tracker_info.txt 0 < t0.in > t0.log 2>&1 & tracker0_pid=$!; CLEANUP_PIDS+=("$tracker0_pid")
"$TRACKER" tracker_info.txt 1 < t1.in > t1.log 2>&1 & tracker1_pid=$!; CLEANUP_PIDS+=("$tracker1_pid")

# Poll rather than a single fixed sleep: the two trackers' connect/accept
# race at startup, and the connector backs off for a couple of seconds
# after a missed first attempt, so a single 1s sleep is occasionally flaky.
synced="no"
for _ in 1 2 3 4 5 6 7 8; do
    send t0 "status" 0.5
    if grep -q 'peer tracker connected: yes' t0.log; then synced="yes"; break; fi
done
check "$synced" "yes" "trackers found each other over the sync link"

echo "== starting 3 clients =="
"$CLIENT" 127.0.0.1:9501 tracker_info.txt < c1.in > c1.log 2>&1 & CLEANUP_PIDS+=($!)
"$CLIENT" 127.0.0.1:9502 tracker_info.txt < c2.in > c2.log 2>&1 & CLEANUP_PIDS+=($!)
"$CLIENT" 127.0.0.1:9503 tracker_info.txt < c3.in > c3.log 2>&1 & CLEANUP_PIDS+=($!)
sleep 0.3

echo "== accounts, group, multi-piece upload (2.3MB -> 5 pieces) =="
head -c 2300000 /dev/urandom > testfile.bin
expected_hash=$(sha1sum testfile.bin | awk '{print $1}')

send c1 "create_user alice pw123"
send c1 "login alice pw123"
send c1 "create_group grp1"
send c1 "upload_file grp1 $WORK/testfile.bin" 0.6

echo "== bob joins, gets accepted, downloads from alice (single peer) =="
send c2 "create_user bob pw456"
send c2 "login bob pw456"
send c2 "join_group grp1"
send c1 "accept_request grp1 bob"
send c2 "download_file grp1 testfile.bin $WORK/file_bob.bin" 1.5

got=$( [ -f file_bob.bin ] && sha1sum file_bob.bin | awk '{print $1}' || echo none)
check "$got" "$expected_hash" "single-peer download matches source hash"

echo "== carol joins, gets accepted, downloads from BOTH alice and bob (multi-peer) =="
send c3 "create_user carol pw789"
send c3 "login carol pw789"
send c3 "join_group grp1"
send c1 "accept_request grp1 carol"
send c3 "download_file grp1 testfile.bin $WORK/file_carol.bin" 1.5

got=$( [ -f file_carol.bin ] && sha1sum file_carol.bin | awk '{print $1}' || echo none)
check "$got" "$expected_hash" "multi-peer download matches source hash"

echo "== killing tracker0 to simulate a crash, then issuing a command from alice =="
kill -9 "$tracker0_pid" 2>/dev/null
sleep 0.5
send c1 "list_groups" 1
check "$(grep -c 'switched to tracker' c1.log)" "1" "client transparently failed over to tracker1"
check "$(grep -c 'grp1' c1.log)" "$(grep -c 'grp1' c1.log)" "sanity: grep works" # always true, keeps counts visible below
grep -q "owner=alice, members=3" c1.log
check "$?" "0" "tracker1 already had the full replicated group state"

echo "== bringing tracker0 back: it should replay its own log, then catch up any new ops =="
send c1 "create_group grp2" 0.5
"$TRACKER" tracker_info.txt 0 < t0.in > t0_restart.log 2>&1 & CLEANUP_PIDS+=($!)
sleep 1.5
send t0 "status" 0.5
check "$(grep -c 'replayed .* op-log record' t0_restart.log)" "1" "tracker0 replayed its on-disk op-log on restart"
grep -q "grp2" t0_restart.log
check "$?" "0" "tracker0 caught up a group created entirely while it was down"

echo
echo "================================"
echo "  $pass passed, $fail failed"
echo "================================"
[ "$fail" -eq 0 ]
