package redisemaphore_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
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
	var renewals atomic.Int64
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
		Observer: redisemaphore.ObserverFunc(func(_ context.Context, event redisemaphore.Event) {
			if event.Type == redisemaphore.EventLeaseRenewed {
				renewals.Add(1)
			}
		}),
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
		holderResult <- sem.Run(ctx, redisemaphore.AcquireRequest{
			Queue: "batch", RequestID: "holder",
		}, func(workCtx context.Context) error {
			close(holderStarted)
			select {
			case <-releaseHolder:
				return nil
			case <-workCtx.Done():
				return workCtx.Err()
			}
		})
	}()
	select {
	case <-holderStarted:
	case <-time.After(time.Second):
		t.Fatal("holder callback did not start")
	}

	contenderStarted := make(chan struct{})
	contenderResult := make(chan error, 1)
	go func() {
		contenderResult <- sem.Run(ctx, redisemaphore.AcquireRequest{
			Queue: "interactive", RequestID: "contender",
		}, func(context.Context) error {
			close(contenderStarted)
			return nil
		})
	}()
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 1 && snapshot.Waiters == 1
	})

	// The holder remains exclusive for more than two original permit TTLs.
	// Without renewal the contender would enter during this interval.
	time.Sleep(2*config.PermitTTL + 100*time.Millisecond)
	select {
	case <-contenderStarted:
		t.Fatal("contender entered while renewable holder callback was still running")
	default:
	}
	if renewals.Load() < 2 {
		t.Fatalf("successful renewal events = %d, want at least 2", renewals.Load())
	}

	close(releaseHolder)
	if err := receiveError(t, holderResult, time.Second); err != nil {
		t.Fatalf("holder Run(): %v", err)
	}
	select {
	case <-contenderStarted:
	case <-time.After(time.Second):
		t.Fatal("contender did not enter after holder returned")
	}
	if err := receiveError(t, contenderResult, time.Second); err != nil {
		t.Fatalf("contender Run(): %v", err)
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
	run := func(requestID string) error {
		return sem.Run(context.Background(), redisemaphore.AcquireRequest{
			Queue: "default", RequestID: requestID,
		}, func(context.Context) error { return nil })
	}
	if err := run("before-flush"); err != nil {
		t.Fatalf("Run() before SCRIPT FLUSH: %v", err)
	}
	mutex, err := redisemaphore.NewMutex(client, redisemaphore.MutexConfig{
		Namespace:      "integration-script-flush-mutex-" + uuid.NewString(),
		AcquireTimeout: 2 * time.Second,
		LeaseTTL:       600 * time.Millisecond,
		PollInitial:    10 * time.Millisecond,
		PollMax:        40 * time.Millisecond,
		CleanupTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewMutex(): %v", err)
	}
	runMutex := func(requestID string) error {
		return mutex.Run(context.Background(), requestID, func(context.Context) error { return nil })
	}
	if err := runMutex("before-flush"); err != nil {
		t.Fatalf("Mutex.Run() before SCRIPT FLUSH: %v", err)
	}
	if err := client.ScriptFlush(context.Background()).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	if err := run("after-flush"); err != nil {
		t.Fatalf("Run() after SCRIPT FLUSH: %v", err)
	}
	if err := runMutex("after-flush"); err != nil {
		t.Fatalf("Mutex.Run() after SCRIPT FLUSH: %v", err)
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
	err = sem.Run(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "read-only-semaphore",
	}, func(workCtx context.Context) error {
		snapshot, snapshotErr := sem.Snapshot(workCtx)
		if snapshotErr != nil {
			return snapshotErr
		}
		if snapshot.Holders != 1 {
			return fmt.Errorf("holders in callback = %d, want 1", snapshot.Holders)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Semaphore.Run() with ReadOnly Cluster client: %v", err)
	}

	mutex, err := redisemaphore.NewMutex(client, redisemaphore.MutexConfig{
		Namespace:      "integration-read-only-cluster-mutex-" + uuid.NewString(),
		AcquireTimeout: 2 * time.Second,
		LeaseTTL:       600 * time.Millisecond,
		PollInitial:    10 * time.Millisecond,
		PollMax:        40 * time.Millisecond,
		CleanupTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewMutex() with ReadOnly Cluster client: %v", err)
	}
	if err := mutex.Run(context.Background(), "read-only-mutex", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Mutex.Run() with ReadOnly Cluster client: %v", err)
	}
}
