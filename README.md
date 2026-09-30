# P2P Distributed File Sharing System

A BitTorrent-style file sharing system with two replicated tracker servers, built in C++17 for AOS Assignment 3 (IIIT Hyderabad). Files are split into 512KB pieces, hashed with SHA-1, and transferred directly between clients. The two trackers keep the same user/group/file state in sync and the system keeps working if one of them goes down.

No external libraries — just POSIX sockets, threads, and a hand-written SHA-1.

## Layout

```
common/     shared code: wire protocol, SHA-1, socket helpers
tracker/    tracker server (state, replication, client-facing server)
client/     client (tracker connection, peer server, downloader, uploader, REPL)
test/       smoke_test.sh — automated end-to-end test
tracker_info.txt   sample 2-tracker config
```

## Build

```
make
make clean
make test   # builds and runs an automated end-to-end test
```

## Run

`tracker_info.txt` has two lines, one per tracker:

```
<ip>:<client_port>:<sync_port>
```

Start the trackers:

```
./tracker/tracker tracker_info.txt 0
./tracker/tracker tracker_info.txt 1
```

Start a client. The `IP:PORT` is the address other peers will use to reach it:

```
./client/client 127.0.0.1:9501 tracker_info.txt
```

Commands: `create_user`, `login`, `create_group`, `join_group`, `leave_group`, `list_groups`, `list_requests`, `accept_request`, `logout`, `upload_file`, `list_files`, `download_file`, `show_downloads`, `stop_share`.

## Example

Terminal 1 and 2: start the two trackers as shown above.

Terminal 3 — alice uploads a file:

```
./client/client 127.0.0.1:9501 tracker_info.txt
create_user alice pw123
login alice pw123
create_group movies
upload_file movies /path/to/some/file.mkv
```

Terminal 4 — bob joins the group and downloads it:

```
./client/client 127.0.0.1:9502 tracker_info.txt
create_user bob pw456
login bob pw456
join_group movies
```

Back in alice's terminal, approve bob:

```
accept_request movies bob
```

Back in bob's terminal:

```
download_file movies file.mkv /tmp/file.mkv
show_downloads
```

Once it's done, `show_downloads` prints `[C] [movies] file.mkv` and bob is now seeding the file too, so a third client could download it from either of them.

## How it's built

**Trackers.** Every change — a new user, a new group, a file being shared, a join request being accepted — is written to an append-only log before it's applied. Each tracker keeps its own counter, so every log entry is tagged with (tracker id, counter value). On startup a tracker replays its own log to rebuild its state. When it connects to the other tracker, they compare counters and each sends the other whatever it's missing. If both trackers get a conflicting write at the same time (say, the same group name created on both sides during a network split), the one with the lower (counter, tracker id) wins on both sides, so they end up agreeing regardless of which one applied it first.

**Downloads.** A client asks the tracker for a file's piece hashes and a list of peers, connects to a few of them, asks each which pieces it has, and downloads the rarest pieces first instead of going in order. That matters because if the only peer holding a particular piece disconnects, getting that piece early is the difference between finishing the download and getting stuck. Every piece is checked against its SHA-1 hash as soon as it arrives, and corrupted or missing pieces get retried from another peer.

**Protocol.** Tracker-client, tracker-tracker, and client-client all use the same simple message format: a small header (type, length) followed by length-prefixed fields. No text parsing or delimiters — every field says its own length.

**Failover.** If a client's tracker connection dies, it reconnects to the other tracker and logs back in automatically.

## Testing

`test/smoke_test.sh` starts two trackers and three clients as real processes, uploads a file, downloads it (first from one peer, then from two peers at once), kills a tracker mid-test to check failover, then restarts it to check recovery. All 8 checks pass. Also ran a multi-peer download under AddressSanitizer to check for concurrency bugs.

## Known limitations

- Passwords are hashed client-side with SHA-1 before being sent, but there's no per-user salt — fine for this assignment, not how a real system should store passwords.
- No resume if the client process itself restarts mid-download.
- No encryption on the wire.
- The conflict resolution is designed around exactly two trackers; it wouldn't extend cleanly to more without extra work.
