# QA Checklist

## Prerequisites

Start the server with a clean data directory:

```bash
cargo build -p server 2>&1 | tail -5
RUST_LOG=info DATA_DIR=./tmp/data JOBS_DIR=./tmp/jobs cargo run --bin server
```

---

## Issue #22 — Job Cancel + Results

### Test 16 — Cancel a queued job

Hold the only semaphore slot with a first job, then cancel the second job while it waits in the queue.

```bash
# Terminal 1 — start server with only 1 concurrent job slot
RUST_LOG=info DATA_DIR=./tmp/data JOBS_DIR=./tmp/jobs MAX_CONCURRENT_JOBS=1 cargo run --bin server

# Terminal 2 — flush some events so jobs have something to query
curl -s -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{"service":"svc","timestamp":1700000000000000000,"kafka_partition":0,"kafka_offset":1,"level":"info","message":"hello"}'

# Submit a long-ish job (job A — will occupy the one slot)
JOB_A=$(curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM hot WHERE service='"'"'svc'"'"'","service":"svc","time_from":0,"time_to":9223372036854775807}' | jq -r .id)
echo "Job A: $JOB_A"

# Submit a second job (job B — will be Queued because slot is full)
JOB_B=$(curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM hot WHERE service='"'"'svc'"'"'","service":"svc","time_from":0,"time_to":9223372036854775807}' | jq -r .id)
echo "Job B: $JOB_B"

# Confirm job B is queued
curl -s http://localhost:8080/jobs/$JOB_B | jq .
```

Expected: `{"status":"queued"}`

```bash
# Cancel job B
curl -s -X DELETE http://localhost:8080/jobs/$JOB_B -w "\nHTTP %{http_code}\n"
```

Expected: HTTP 204, empty body.

```bash
# Confirm job B is now cancelled
curl -s http://localhost:8080/jobs/$JOB_B | jq .
```

Expected: `{"status":"cancelled"}`

---

### Test 17 — Cancel a completed job returns 409

```bash
# Submit a job and wait for it to complete
JOB=$(curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM hot WHERE service='"'"'svc'"'"'","service":"svc","time_from":0,"time_to":9223372036854775807}' | jq -r .id)

# Poll until complete (fast — usually under 1 second)
for i in $(seq 1 10); do
  STATUS=$(curl -s http://localhost:8080/jobs/$JOB | jq -r .status)
  echo "Status: $STATUS"
  [ "$STATUS" = "complete" ] && break
  sleep 0.5
done

# Attempt to cancel the completed job
curl -s -X DELETE http://localhost:8080/jobs/$JOB -w "\nHTTP %{http_code}\n"
```

Expected: HTTP 409, body `{"error":"job already in terminal state"}`

---

### Test 18 — Cancel an already-cancelled job returns 409

```bash
# Use job B from Test 16 (already cancelled), or cancel a new queued job first, then try again
curl -s -X DELETE http://localhost:8080/jobs/$JOB_B -w "\nHTTP %{http_code}\n"
```

Expected: HTTP 409, body `{"error":"job already in terminal state"}`

---

### Test 19 — Results pagination

```bash
# Flush 5 distinct events
for i in 1 2 3 4 5; do
  curl -s -X POST http://localhost:8080/ingest \
    -H "Content-Type: application/json" \
    -d "{\"service\":\"pager\",\"timestamp\":$((1700000000000000000 + i)),\"kafka_partition\":0,\"kafka_offset\":$i,\"level\":\"info\",\"message\":\"msg$i\"}"
done

# Submit a job over those events
JOB=$(curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM hot WHERE service='"'"'pager'"'"' ORDER BY kafka_offset","service":"pager","time_from":0,"time_to":9223372036854775807}' | jq -r .id)

# Wait for completion
sleep 1
curl -s http://localhost:8080/jobs/$JOB | jq .
```

Expected: `{"status":"complete","result_count":5}`

```bash
# Page 0 — 2 rows
curl -s "http://localhost:8080/jobs/$JOB/results?page=0&page_size=2" | jq .
```

Expected: `{"page":0,"page_size":2,"total":5,"rows":[...]}` with 2 rows.

```bash
# Page 1 — 2 rows
curl -s "http://localhost:8080/jobs/$JOB/results?page=1&page_size=2" | jq .
```

Expected: `{"page":1,"page_size":2,"total":5,"rows":[...]}` with 2 rows.

```bash
# Page 2 — 1 row (last)
curl -s "http://localhost:8080/jobs/$JOB/results?page=2&page_size=2" | jq .
```

Expected: `{"page":2,"page_size":2,"total":5,"rows":[...]}` with 1 row.

```bash
# Page 3 — past end, 0 rows
curl -s "http://localhost:8080/jobs/$JOB/results?page=3&page_size=2" | jq .
```

Expected: `{"page":3,"page_size":2,"total":5,"rows":[]}` with 0 rows.

---

### Test 20 — Results on a non-complete job returns 409

```bash
# With MAX_CONCURRENT_JOBS=1 and job A still running (or use a queued job B):
curl -s http://localhost:8080/jobs/$JOB_B/results -w "\nHTTP %{http_code}\n"
```

Expected: HTTP 409, body `{"error":"job is not complete"}`

---

### Test 21 — Semaphore limits concurrency

```bash
# Start server with 1 slot
MAX_CONCURRENT_JOBS=1 RUST_LOG=info DATA_DIR=./tmp/data JOBS_DIR=./tmp/jobs cargo run --bin server

# Submit two jobs back-to-back
JOB1=$(curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM hot WHERE service='"'"'svc'"'"'","service":"svc","time_from":0,"time_to":9223372036854775807}' | jq -r .id)
JOB2=$(curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM hot WHERE service='"'"'svc'"'"'","service":"svc","time_from":0,"time_to":9223372036854775807}' | jq -r .id)

curl -s http://localhost:8080/jobs/$JOB1 | jq .status
curl -s http://localhost:8080/jobs/$JOB2 | jq .status
```

Expected: JOB1 is `"running"` or `"complete"`, JOB2 is `"queued"` (held back by the semaphore).

After JOB1 completes, JOB2 should automatically transition to `"running"` then `"complete"`.

---

### Test 22 — Unknown job ID returns 404

```bash
curl -s http://localhost:8080/jobs/00000000-0000-0000-0000-000000000000 -w "\nHTTP %{http_code}\n"
curl -s -X DELETE http://localhost:8080/jobs/00000000-0000-0000-0000-000000000000 -w "\nHTTP %{http_code}\n"
curl -s http://localhost:8080/jobs/00000000-0000-0000-0000-000000000000/results -w "\nHTTP %{http_code}\n"
```

Expected: HTTP 404 for all three.

---

### Note on cancelling a running job

DataFusion queries over small datasets complete in microseconds, making it impractical to race a DELETE against a running job in a manual test. The automated integration tests cover this path deterministically by holding the semaphore permit to keep the job in `Queued` state before issuing the cancel.

To attempt it manually, add a temporary `tokio::time::sleep(Duration::from_secs(10)).await` at the top of `run_job` in `server/src/main.rs`, recompile, then:

1. Submit a job (it will enter `Running` and sleep 10 s).
2. `DELETE /jobs/:id` within the 10-second window.
3. Confirm the status flips to `cancelled` and the background task does not overwrite it with `complete`.

Remove the sleep before committing.

---

## Automated tests

All issue #22 paths are covered by the integration tests in `server/src/main.rs`:

```bash
cargo test -p server 2>&1 | grep -E "test .* (ok|FAILED)"
```

Key tests:
- `cancel_queued_job_transitions_to_cancelled_and_releases_semaphore`
- `delete_complete_job_returns_409`
- `results_endpoint_paginates_correctly`
