# Redis-Based Distributed Semaphore Implementation, With Priority Queues

This repository contains a Go implementation of a Redis-based semaphore mechanism that allows for distributed locking using Redis sorted sets. It also includes a mutex lock mechanism to ensure safe concurrent access. The distribution of the semaphore is based redis clusters, i.e. we rely on the correctness of the Redis cluster to ensure the semaphore's correctness.

The semaphore itself has priorty queues, which allows tasks to be scheduled in a specific order. This is useful for tasks that are preferred to be executed first, such as tasks that are more time-critical or tasks that have higher priority.

## Features

1. **Semaphore Acquisition and Release:**
   - Acquire and release semaphores directly or via named queues.
   - Configurable semaphore options (expiry, timeout, polling duration, etc.).

2. **Mutex Locking:**
   - Acquire and release mutex locks to prevent race conditions during semaphore operations.
   - Configurable lock options (expiry, timeout, polling duration, etc.).

## Installation

```shell
go get github.com/amitaifrey/redisemaphore
```

## Getting Started
### Prerequisites
- Go (v1.18+)
- Redis

### Example Usage 

```go
package main

import (
	"context"
    "log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yourusername/redisemaphore"
)

func main() {
	ctx := context.Background()
	client := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"localhost:6379"}})

	// Initialize semaphore
	sem, err := redisemaphore.NewSemaphore(client, "example-semaphore", 5,
		redisemaphore.WithSemaphoreMutexExpiry(2*time.Minute),
		redisemaphore.WithSemaphorePollDur(200*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Acquire semaphore
	err = sem.Acquire(ctx, "my-key")
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

## Configuration Options

### Semaphore Options

- `WithSemaphoreMutexName(name string)`: Set a custom name for the mutex.
- `WithSemaphoreMutexExpiry(expiry time.Duration)`: Set the expiry duration for the mutex.
- `WithSemaphoreMutexTimeout(timeout time.Duration)`: Set the timeout duration for acquiring the mutex.
- `WithSemaphoreDeleteTimeout(deleteTimeout time.Duration)`: Set the timeout duration for deleting keys from the semaphore set.
- `WithSemaphorePollDur(pollDur time.Duration)`: Set the polling duration for the semaphore.
- `WithSemaphoreQueueKeysByPrio(queueKeysByPrio ...string)`: Set the priority queue keys for the semaphore.

### Mutex Options

- `WithMutexPollDur(pollDur time.Duration)`: Set the polling duration for the mutex.
- `WithMutexExpiry(expiry time.Duration)`: Set the expiry duration for the mutex.
- `WithMutexTimeout(timeout time.Duration)`: Set the timeout duration for acquiring the mutex.

## Error Handling

Common errors:
- `ErrNoKeysLeft`: Indicates that no keys are left in the queues.
- `ErrTimeout`: Indicates that a lock acquisition has timed out.

## Contributing

We welcome contributions! Please fork the repository and submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.

## Acknowledgements

- [Go-Redis](https://github.com/redis/go-redis): A Go client for Redis.

---

By using this repository, you agree to the terms and conditions of the accompanying License.