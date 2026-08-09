package redisemaphore_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/amitaifrey/redisemaphore"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func integrationRedisClient(t *testing.T) redis.UniversalClient {
	t.Helper()
	addrs := integrationRedisAddrs(t)

	client := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: addrs})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close integration Redis client: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping integration Redis at %v: %v", addrs, err)
	}
	return client
}

func integrationRedisAddrs(t *testing.T) []string {
	t.Helper()

	rawAddrs := strings.TrimSpace(os.Getenv("REDIS_ADDR"))
	if rawAddrs == "" {
		t.Skip("set REDIS_ADDR to run real-Redis integration tests")
	}
	parts := strings.Split(rawAddrs, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		if addr := strings.TrimSpace(part); addr != "" {
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) == 0 {
		t.Fatal("REDIS_ADDR did not contain a usable address")
	}
	return addrs
}

func TestIntegrationSemaphoreRenewsWithoutOverAdmission(t *testing.T) {
	client := integrationRedisClient(t)
	config := redisemaphore.SemaphoreConfig{
		Namespace:        "integration-{braces}-" + uuid.NewString(),
		Capacity:         1,
		QueuesByPriority: []string{"interactive", "batch"},
		AcquireTimeout:   3 * time.Second,
		PermitTTL:        300 * time.Millisecond,
		WaiterTTL:        200 * time.Millisecond,
		PollInitial:      10 * time.Millisecond,
		PollMax:          40 * time.Millisecond,
		CleanupTimeout:   50 * time.Millisecond,
	}
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderResult := make(chan error, 1)
	go func() {
		permit, acquireErr := sem.Acquire(ctx, redisemaphore.AcquireRequest{
			Queue: "batch", RequestID: "holder",
		})
		if acquireErr != nil {
			holderResult <- acquireErr
			return
		}
		close(holderStarted)
		var workErr error
		select {
		case <-releaseHolder:
		case <-permit.Context().Done():
			workErr = context.Cause(permit.Context())
		}
		holderResult <- errors.Join(workErr, permit.Release())
	}()
	select {
	case <-holderStarted:
	case <-time.After(time.Second):
		t.Fatal("holder did not acquire a permit")
	}

	contenderStarted := make(chan struct{})
	contenderResult := make(chan error, 1)
	go func() {
		permit, acquireErr := sem.Acquire(ctx, redisemaphore.AcquireRequest{
			Queue: "interactive", RequestID: "contender",
		})
		if acquireErr != nil {
			contenderResult <- acquireErr
			return
		}
		close(contenderStarted)
		contenderResult <- permit.Release()
	}()
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 1 && snapshot.Waiters == 1
	})

	// The holder remains exclusive for more than two original permit TTLs.
	// Without renewal the contender would enter during this interval.
	time.Sleep(2*config.PermitTTL + 100*time.Millisecond)
	select {
	case <-contenderStarted:
		t.Fatal("contender entered while renewable holder permit was still held")
	default:
	}

	close(releaseHolder)
	if err := receiveError(t, holderResult, time.Second); err != nil {
		t.Fatalf("holder lifecycle: %v", err)
	}
	select {
	case <-contenderStarted:
	case <-time.After(time.Second):
		t.Fatal("contender did not enter after holder returned")
	}
	if err := receiveError(t, contenderResult, time.Second); err != nil {
		t.Fatalf("contender lifecycle: %v", err)
	}
}

func TestIntegrationScriptsReloadAfterFlush(t *testing.T) {
	if os.Getenv("REDIS_ALLOW_SCRIPT_FLUSH") != "1" {
		t.Skip("set REDIS_ALLOW_SCRIPT_FLUSH=1 for the isolated Redis test server")
	}
	client := integrationRedisClient(t)
	config := redisemaphore.SemaphoreConfig{
		Namespace:        "integration-script-flush-" + uuid.NewString(),
		Capacity:         1,
		QueuesByPriority: []string{"default"},
		AcquireTimeout:   2 * time.Second,
		PermitTTL:        600 * time.Millisecond,
		WaiterTTL:        300 * time.Millisecond,
		PollInitial:      10 * time.Millisecond,
		PollMax:          40 * time.Millisecond,
		CleanupTimeout:   50 * time.Millisecond,
	}
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}
	acquireAndRelease := func(requestID string) error {
		permit, acquireErr := sem.Acquire(context.Background(), redisemaphore.AcquireRequest{
			Queue: "default", RequestID: requestID,
		})
		if acquireErr != nil {
			return acquireErr
		}
		return permit.Release()
	}
	if err := acquireAndRelease("before-flush"); err != nil {
		t.Fatalf("Acquire/Release before SCRIPT FLUSH: %v", err)
	}
	if err := client.ScriptFlush(context.Background()).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	if err := acquireAndRelease("after-flush"); err != nil {
		t.Fatalf("Acquire/Release after SCRIPT FLUSH: %v", err)
	}
}

func TestIntegrationReadOnlyClusterRoutesOwnershipScriptsToPrimary(t *testing.T) {
	addrs := integrationRedisAddrs(t)
	if len(addrs) < 6 {
		t.Skip("requires the six-node Redis Cluster integration job with replicas")
	}
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrs,
		ReadOnly: true,
	})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close read-only Cluster client: %v", err)
		}
	})

	config := redisemaphore.SemaphoreConfig{
		Namespace:        "integration-read-only-cluster-" + uuid.NewString(),
		Capacity:         1,
		QueuesByPriority: []string{"default"},
		AcquireTimeout:   2 * time.Second,
		PermitTTL:        600 * time.Millisecond,
		WaiterTTL:        300 * time.Millisecond,
		PollInitial:      10 * time.Millisecond,
		PollMax:          40 * time.Millisecond,
		CleanupTimeout:   100 * time.Millisecond,
	}
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore() with ReadOnly Cluster client: %v", err)
	}
	permit, err := sem.Acquire(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "read-only-semaphore",
	})
	if err != nil {
		t.Fatalf("Semaphore.Acquire() with ReadOnly Cluster client: %v", err)
	}
	snapshot, err := sem.Snapshot(permit.Context())
	if err != nil {
		t.Fatalf("Snapshot() with ReadOnly Cluster client: %v", err)
	}
	if snapshot.Holders != 1 {
		t.Fatalf("holders while permit held = %d, want 1", snapshot.Holders)
	}
	if err := permit.Release(); err != nil {
		t.Fatalf("Permit.Release() with ReadOnly Cluster client: %v", err)
	}
}
