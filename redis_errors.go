package redisemaphore

import (
	"errors"
	"reflect"
	"strings"

	"github.com/redis/go-redis/v9"
)

func nilRedisClient(redisClient redis.UniversalClient) bool {
	if redisClient == nil {
		return true
	}
	value := reflect.ValueOf(redisClient)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isRedisServerError(err error) bool {
	// These package-level errors also prove that the state transaction did not
	// commit. Treat them like definitive Redis replies for ambiguity tracking.
	if errors.Is(err, redis.TxFailedErr) ||
		errors.Is(err, errOperationGateBusy) ||
		errors.Is(err, errOperationGateLost) {
		return true
	}
	var serverErr redis.Error
	return errors.As(err, &serverErr)
}

// go-redis retries most of these internally, but a failover or a transaction
// can outlast its configured retry budget. Keep polling within the
// operation's deadline instead of turning a transient Redis state into an
// immediate failure. Other Redis replies (for example WRONGTYPE and
// authentication errors) are deterministic and fail fast.
func isRetryableRedisServerError(err error) bool {
	for _, prefix := range []string{
		"ASK",
		"BUSY",
		"CLUSTERDOWN",
		"LOADING",
		"MASTERDOWN",
		"MOVED",
		"NOREPLICAS",
		"READONLY",
		"TRYAGAIN",
	} {
		if redis.HasErrorPrefix(err, prefix) {
			return true
		}
	}
	return strings.Contains(err.Error(), "max number of clients reached")
}

func isRetryableRedisOperationError(err error) bool {
	if err == nil {
		return false
	}
	if transactionOutcomeUncertain(err) {
		return true
	}
	// WATCH conflicts are definitive non-commits, but remain retryable within
	// the caller's overall operation budget even if one attempt also reached
	// its local deadline.
	if errors.Is(err, redis.TxFailedErr) ||
		errors.Is(err, errOperationGateBusy) ||
		errors.Is(err, errOperationGateLost) {
		return true
	}
	if errors.Is(err, redis.ErrClosed) ||
		errors.Is(err, redis.Nil) ||
		errors.Is(err, errInvalidSemaphoreState) {
		return false
	}
	if isRedisServerError(err) {
		return isRetryableRedisServerError(err)
	}
	// Non-server errors are normally transport, pool, or per-attempt context
	// failures. Their commit outcome is uncertain, so retry with the same token.
	return true
}
