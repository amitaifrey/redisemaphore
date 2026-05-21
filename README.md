# Redis-Based Distributed Semaphore Implementation, With Priority Queues

This repository contains a Go implementation of a Redis-based semaphore mechanism that allows for distributed locking using Redis sorted sets. It also includes a mutex lock mechanism to ensure safe concurrent access. The distribution of the semaphore is based on Redis, i.e. we rely on Redis atomic commands and Lua script atomicity to ensure the semaphore's correctness.

The semaphore itself has priority queues, which allow tasks to be scheduled in a specific order. This is useful for tasks that are preferred to be executed first, such as tasks that are more time-critical or tasks that have higher priority.

## Features

1. **Semaphore Acquisition and Release:**
   - Acquire directly or via logical priority queue IDs.
   - Release by semaphore key; release is independent of the queue used to acquire.
   - Internally derived Redis keys share one hash tag, making semaphore promotion scripts Redis Cluster-safe by default.
   - Semaphore keys are unique acquisition tokens; use a different key for each independent permit.
   - Pods sharing one semaphore namespace must use the same size, queue IDs/order, permit TTL, and mutex expiry.

2. **Mutex Locking:**
   - Acquire and release mutex locks to prevent race conditions during semaphore operations.
   - Configurable lock options (expiry, timeout, polling duration, etc.).
   - Standalone mutex namespaces are separate from semaphore-internal mutexes.

## Installation

```shell
go get github.com/amitaifrey/redisemaphore
```

## Getting Started
### Prerequisites
- Go 1.22+
- Redis

### Example Usage 

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/amitaifrey/redisemaphore"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()
	client := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"localhost:6379"}})

	// Initialize semaphore
	sem, err := redisemaphore.NewSemaphore(client, "example-semaphore", 5,
		redisemaphore.WithSemaphoreQueuesByPriority("high", "default"),
		redisemaphore.WithSemaphoreMutexExpiry(2*time.Minute),
		redisemaphore.WithSemaphorePollDur(200*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Acquire semaphore via a logical queue ID
	err = sem.AcquireQueue(ctx, "high", "my-key")
	if err != nil {
		log.Fatal(err)
	}

	// Do some work...

	// Release semaphore
	err = sem.Release(ctx, "my-key")
	if err != nil {
		log.Fatal(err)
	}
}
```

The namespace and queue IDs are logical names, not raw Redis keys. Every pod that uses the same semaphore namespace must use the same semaphore configuration: size, queue IDs and order, permit TTL, and mutex expiry. Use a new namespace or clear that namespace's Redis keys before changing those values.

Standalone mutexes created with `NewMutex` use a separate internal key space from semaphore-internal mutexes, so `NewMutex(client, "jobs")` will not block `NewSemaphore(client, "jobs", ...)`.

## Configuration Options

### Semaphore Options

- `WithSemaphoreMutexExpiry(expiry time.Duration)`: Set the expiry duration for the mutex.
- `WithSemaphoreMutexTimeout(timeout time.Duration)`: Set the timeout duration for acquiring the mutex.
- `WithSemaphorePermitTTL(permitTTL time.Duration)`: Set how long a held semaphore key can remain unreleased before it is treated as expired and cleaned up.
- `WithSemaphorePollDur(pollDur time.Duration)`: Set the polling duration for the semaphore.
- `WithSemaphoreQueuesByPriority(queueIDs ...string)`: Set logical queue IDs from highest priority to lowest.

### Mutex Options

- `WithMutexPollDur(pollDur time.Duration)`: Set the polling duration for the mutex.
- `WithMutexExpiry(expiry time.Duration)`: Set the expiry duration for the mutex.
- `WithMutexTimeout(timeout time.Duration)`: Set the timeout duration for acquiring the mutex.

## Error Handling

Common errors:
- `ErrInvalidConfig`: Indicates invalid constructor options or invalid acquire/release parameters.
- `ErrTimeout`: Indicates that a lock acquisition has timed out.
- `ErrDuplicateKey`: Indicates that a semaphore key is already waiting or holding a permit.
Config mismatches for an existing semaphore namespace return an error wrapping `ErrInvalidConfig`.

## Contributing

We welcome contributions! Please fork the repository and submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.

## Acknowledgements

- [Go-Redis](https://github.com/redis/go-redis): A Go client for Redis.

---

By using this repository, you agree to the terms and conditions of the accompanying License.
