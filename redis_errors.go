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
	var serverErr redis.Error
	return errors.As(err, &serverErr)
}

// go-redis retries most of these internally, but a failover or a long-running
// script can outlast its configured retry budget. Keep polling within the
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
		"NOSCRIPT",
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
	if errors.Is(err, redis.ErrClosed) ||
		errors.Is(err, redis.Nil) ||
		errors.Is(err, errInvalidSemaphoreScriptResponse) {
		return false
	}
	if isRedisServerError(err) {
		return isRetryableRedisServerError(err)
	}
	// Non-server errors are normally transport, pool, or per-attempt context
	// failures. Their commit outcome is uncertain, so retry with the same token.
	return true
}
