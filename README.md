# redisemaphore

`redisemaphore` provides renewable, callback-scoped Redis semaphores and mutexes for Go. Admission and ownership changes are performed by bounded Lua scripts over a versioned, single-hash-slot keyspace, so every semaphore transition is atomic on one healthy Redis primary and works with Redis Cluster routing.

Version 1 is a breaking redesign. The old `Acquire`/`Release` API and the semaphore's internal distributed mutex are gone. Protected work must run inside `Run`; this makes ownership renewal, cancellation, and token-checked cleanup part of one lifecycle.

## Safety boundary

> [!IMPORTANT]
> A Redis lease is a coordination signal, not a durable fence. After a lease expires or is lost, an old callback can overlap a newly admitted callback until the old work actually stops. Eviction, failover data loss, destructive Redis commands, and restoring older data can also erase acknowledged ownership. For any effect that must not happen twice or out of order, enforce idempotency with a durable application operation ID or enforce a monotonically increasing fence at the protected system. This package does not issue a fencing token.

The package's random ownership token only prevents a delayed Redis cleanup or renewal from modifying a replacement lease. It is neither monotonic nor exposed as a downstream fence. `RequestID` may be part of an application idempotency key if it is recorded durably and not reused for that effect; passing it to `Run` alone does not deduplicate the callback's side effects.

## Requirements

- Go 1.22 or later
- Redis 7 or later
- `github.com/redis/go-redis/v9`

## Installation

```shell
go get github.com/amitaifrey/redisemaphore
```

## Semaphore

```go
package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/amitaifrey/redisemaphore"
	"github.com/redis/go-redis/v9"
)

func main() {
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
	})
	defer client.Close()

	sem, err := redisemaphore.NewSemaphore(client, redisemaphore.SemaphoreConfig{
		Namespace:        "image-processing",
		Capacity:         8,
		QueuesByPriority: []string{"interactive", "batch"},
		AcquireTimeout:   30 * time.Second,
		PermitTTL:        90 * time.Second,
		WaiterTTL:        10 * time.Second,
		PollInitial:      100 * time.Millisecond,
		PollMax:          time.Second,
		CleanupTimeout:   2 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	err = sem.Run(context.Background(), redisemaphore.AcquireRequest{
		Queue:     "interactive",
		RequestID: "job-018f5d7f",
	}, func(workCtx context.Context) error {
		// Use workCtx for every cancellable operation. It is canceled if the
		// caller cancels or Redis ownership can no longer be confirmed.
		return processImage(workCtx)
	})
	if errors.Is(err, redisemaphore.ErrAcquireTimeout) {
		// The callback was never called.
		return
	}
	if errors.Is(err, redisemaphore.ErrLeaseLost) {
		// The callback context was canceled; the work must stop promptly.
		return
	}
	if err != nil {
		log.Fatal(err)
	}
}

func processImage(context.Context) error { return nil }
```

Queue names are registered in descending priority in `QueuesByPriority`. Admission is FIFO within each queue and strictly prefers every live waiter in a higher-priority queue. This is deliberate: a continuously busy high-priority queue can starve lower-priority work. Choose queues and upstream admission limits accordingly.

Queue order and waiter liveness are separate Redis records. A waiter's original sequence is stable for FIFO ordering, while a short renewable waiter lease proves that the process is still polling. Heartbeat scheduling is clamped to one third of `WaiterTTL`, even when the configured polling backoff is longer. Cleanup removes expired waiter leases; it never treats an old enqueue time as proof that a live, long-waiting request is dead. A process pause or Redis outage longer than the waiter TTL makes the lease expire; a caller that later recovers rejoins at the tail.

`RequestID` is an application-visible correlation and duplicate-detection value. It must be non-empty and unique among active requests in a namespace. It may be reused after the earlier call has returned and token cleanup succeeded. If cleanup fails, wait for the old holder or waiter TTL to expire before reusing it. Internal random ownership tokens prevent a delayed old cleanup from releasing the newer request.

The semaphore renews its permit while the callback is running. If it cannot confirm ownership with enough time left for an orderly shutdown, it cancels the callback context and returns an error matching `ErrLeaseLost`. Renewal continues while the callback winds down, retaining capacity until it returns or the lease can no longer be renewed. A callback must stop promptly when its derived context is canceled; ignoring that context can let work continue after ownership is uncertain, so context propagation is a correctness requirement, not an optional convenience. The configured capacity bounds valid Redis permits, not goroutines or side effects that continue after their leases become invalid.

After the callback returns, including with an error or panic, `Run` starts token-checked cleanup and retries only until `CleanupTimeout`. An independent local watchdog bounds the cleanup phase even when the Redis client does not apply context deadlines to socket I/O. If that watchdog expires, cleanup completes with an error, but an already-issued Redis command may still finish later. Such a late cleanup can remove only the same ownership token, never a replacement. The bound therefore applies to caller wait time during cleanup, not to the lifetime of underlying socket I/O and not to successful release; configure finite Redis dial, read, and write timeouts as well. On the normal return path the cleanup error is returned or joined with other errors; a callback panic is re-raised after the bounded cleanup wait. Joined errors remain compatible with `errors.Is`/`errors.As`.

## Mutex

Use the mutex only when the protected operation genuinely needs single-owner execution. Semaphores do not use it internally.

```go
ctx := context.Background()
mutex, err := redisemaphore.NewMutex(client, redisemaphore.MutexConfig{
	Namespace:      "daily-rollup",
	AcquireTimeout: 30 * time.Second,
	LeaseTTL:       60 * time.Second,
	PollInitial:    100 * time.Millisecond,
	PollMax:        time.Second,
	CleanupTimeout: 2 * time.Second,
})
if err != nil {
	log.Fatal(err)
}

err = mutex.Run(ctx, "rollup-2026-08-06", func(workCtx context.Context) error {
	return writeRollup(workCtx)
})
```

The mutex uses the same renewable-lease behavior, lease-loss cancellation, random owner tokens, and compare-and-delete cleanup as the semaphore.

## Configuration and errors

For semaphores, zero-valued duration fields use the incident-consumer defaults: a 30-second acquisition timeout, renewable 90-second holder permit, 10-second renewable waiter lease, 100-millisecond initial poll, 1-second maximum poll, and 2-second cleanup timeout. A 90-second permit is safe for longer callbacks only because `Run` renews it; it is not a maximum callback duration. Mutexes use the same defaults except for a 60-second `LeaseTTL` and no waiter lease. Every nonzero configured duration must be a positive, whole-millisecond value; `PollInitial` must not exceed `PollMax`, and lease-related durations must leave enough time for renewal and cleanup. A semaphore supports at most 64 non-empty, unique queue names.

`AcquireTimeout` bounds polling and is checked after each Redis command returns; it does not asynchronously abandon an in-flight admission command. With go-redis's default `ContextTimeoutEnabled: false`, a stuck socket operation can therefore extend `Run` past `AcquireTimeout` or parent cancellation. Configure finite Redis dial, read, and write timeouts (or enable go-redis context timeouts) when wall-clock return bounds matter. Once the command returns, an expired acquisition is token-cleaned and its callback is not invoked; keeping that cleanup ordered after the in-flight command avoids a late grant being created after cleanup already ran.

Configuration is immutable for a namespace. The first client records a fingerprint containing the schema version, capacity, ordered queues, and lease settings; a client with incompatible settings receives `ErrInvalidConfig`. Use a new namespace to reconfigure a live semaphore.

Exported sentinel errors are:

- `ErrInvalidConfig`: invalid local configuration or a namespace fingerprint mismatch
- `ErrAcquireTimeout`: admission did not complete before `AcquireTimeout`
- `ErrDuplicateRequest`: the same request ID is already active
- `ErrLeaseLost`: the callback's ownership expired or could no longer be confirmed safely

Parent-context cancellation is returned as the corresponding `context` error. Test errors with `errors.Is`; cleanup errors may be joined with another result.

## Observability

`Snapshot(ctx)` returns an approximate diagnostic view of live holder and waiter counts, per-queue waiters, the oldest wait duration, and whether the configured capacity has been exceeded. It samples Redis primary time once, then scans waiter metadata in batches; it is not an atomic transaction, so concurrent admission and cleanup can cause omissions or duplicates and per-queue totals need not equal the sampled global waiter count. Expired entries are excluded at the sampled time. The read cost is linear in stored waiter records, including expired records awaiting cleanup, so call it with a deadline and keep it off hot paths.

Set `Observer` in a config to receive acquisition, renewal, cleanup, pruning, and Redis-error events. Delivery is bounded and best-effort: events may be dropped rather than delaying admission, renewal, cancellation, or cleanup. An observer callback may block without affecting lease correctness, although it should return promptly so the bounded observer delivery capacity remains available. Do not use request IDs as metric labels; keep metric dimensions bounded to values such as queue and outcome.

Recommended alerts are critical on any lease loss or capacity-invariant violation and on sustained Redis script errors. Warn on at least 10 acquisition timeouts in five minutes, qualified by total attempt rate so low-volume noise does not page. Track acquisition latency, oldest waiter, waiters per queue, live callbacks versus holders, expired leases pruned, renewal retries, and script duration.

## Redis guarantees and limitations

All keys for a namespace use the versioned form `redisemaphore:{escaped-namespace}:v1:*`. The hash tag keeps every key touched by one script in the same Redis Cluster slot. Lua execution makes each transition atomic on one healthy primary.

That atomicity guarantee is conditional on the complete package keyspace still being present and unmodified. Configuration, queue, waiter, holder, request mapping, and mutex keys are correctness state even though many are temporary. They must not be evicted, manually deleted, have their TTLs changed, or be modified by another protocol. Set Redis `maxmemory-policy` to `noeviction` for the deployment that stores these keys; preferably use an isolated Redis instance or cluster and monitor memory headroom so unrelated workloads cannot threaten the keyspace. A separate logical database alone is not memory or eviction isolation.

With those conditions, the scripts prevent the primary from granting more live permits than the stored capacity and prevent a stale token from renewing or deleting a replacement token. This is not a guarantee that no more than that many callbacks are physically executing: a callback that does not stop on cancellation can outlive its valid permit. Lease expiry also depends on sane Redis clocks, including across promoted replicas.

Redis replication is asynchronous. A primary can acknowledge a lease transition that is lost during failover before it reaches a replica. RDB snapshots and the usual AOF/replication settings can likewise lose recently acknowledged state after a crash. Consequently, enabling Redis persistence does not by itself make mutual exclusion failover-safe or restart-safe.

`SCRIPT FLUSH` only removes Redis's cached Lua code; the client reloads missing scripts and ownership data remains intact. A restart that reloads the exact dataset and expiration deadlines may preserve the Redis records, but the availability gap can still exhaust clients' renewal safety windows. In contrast, `FLUSHDB`, `FLUSHALL`, manual key deletion, a restart that loses state, restoring an older backup, or any other dataset rollback invalidates the coordination state. A restored dataset can omit a current owner or resurrect obsolete bookkeeping. Do not perform those destructive operations while relying on active leases. Stop admission, cancel and drain all protected callbacks, then perform maintenance; after a rollback or restore, resume with a fresh namespace.

Use a durable idempotency record or downstream fencing/version check for correctness-critical effects, including during ordinary lease loss as well as failover and recovery. Redis persistence improves availability and recovery but does not replace that application-level protection.

## Migrating from the pre-v1 API

The v1 keyspace is intentionally disjoint from old keys, but old and v1 consumers must never enforce the same logical limit concurrently.

1. Deploy code that understands v1 with the new path disabled.
2. Pause new admission and let all old holders, queues, mutex keys, and protected callbacks drain to zero.
3. Switch every consumer together to a fresh v1 namespace and resume traffic.
4. Remove the old path only after the new metrics are healthy.

A rollback also requires pausing and draining v1 first. Do not hot-roll back to old keys, and do not reuse an existing namespace when changing capacity, queue order, or lease settings.

## Testing with Redis

The default test suite uses an in-process Redis substitute and needs no daemon:

```shell
go test ./...
go test -race ./...
```

The integration tests use a real Redis server only when `REDIS_ADDR` is set. They isolate state with random namespaces and do not flush the selected database:

```shell
REDIS_ADDR=127.0.0.1:6379 go test ./... -run Integration
```

The script-reload test calls the server-wide `SCRIPT FLUSH` command and is separately skipped unless `REDIS_ALLOW_SCRIPT_FLUSH=1`. Enable it only for an isolated test server:

```shell
REDIS_ADDR=127.0.0.1:6379 REDIS_ALLOW_SCRIPT_FLUSH=1 go test ./... -run IntegrationScriptsReload
```

Run integration coverage against every Redis standalone and Cluster version supported by your deployment. For Cluster, set `REDIS_ADDR` to a comma-separated list of node addresses; the tests construct the same `redis.UniversalClient` shape used by the package.

## License

Apache License 2.0. See [LICENSE](LICENSE).
