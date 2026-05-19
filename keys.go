package redisemaphore

import (
	"fmt"
	"net/url"
	"reflect"
	"strings"

	"github.com/redis/go-redis/v9"
)

const redisKeyPrefix = "redisemaphore"
const redisClusterSlots = 16384

func invalidConfig(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfig, fmt.Sprintf(format, args...))
}

func redisMutexKey(namespace string) string {
	return redisKey(namespace, "mutex")
}

func redisHolderKey(namespace string) string {
	return redisKey(namespace, "holders")
}

func redisQueueKey(namespace, queueID string) string {
	return redisKey(namespace, "queue:"+escapeRedisKeyPart(queueID))
}

func redisKey(namespace, suffix string) string {
	return fmt.Sprintf("%s:{%s}:%s", redisKeyPrefix, escapeRedisKeyPart(namespace), suffix)
}

func escapeRedisKeyPart(part string) string {
	return url.PathEscape(part)
}

func redisClusterSlot(key string) int {
	var crc uint16
	for _, b := range []byte(redisHashKey(key)) {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return int(crc) % redisClusterSlots
}

func redisHashKey(key string) string {
	start := strings.IndexByte(key, '{')
	if start < 0 {
		return key
	}
	endOffset := strings.IndexByte(key[start+1:], '}')
	if endOffset <= 0 {
		return key
	}
	return key[start+1 : start+1+endOffset]
}

func isNilRedisClient(client redis.UniversalClient) bool {
	if client == nil {
		return true
	}
	value := reflect.ValueOf(client)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
