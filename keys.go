package redisemaphore

import (
	"fmt"
	"net/url"
)

const redisKeyPrefix = "redisemaphore"

type semaphoreKeys struct {
	config        string
	sequence      string
	holders       string
	waiters       string
	waiterStarted string
	tokenQueue    string
	tokenRequest  string
	activeRequest string
	queues        []string
}

func newSemaphoreKeys(namespace string, queues []string) semaphoreKeys {
	base := fmt.Sprintf("%s:{%s}:v1:semaphore", redisKeyPrefix, escapeKeyPart(namespace))
	keys := semaphoreKeys{
		config:        base + ":config",
		sequence:      base + ":sequence",
		holders:       base + ":holders",
		waiters:       base + ":waiters",
		waiterStarted: base + ":waiter-started",
		tokenQueue:    base + ":token-queue",
		tokenRequest:  base + ":token-request",
		activeRequest: base + ":active-request",
		queues:        make([]string, len(queues)),
	}
	for i, queue := range queues {
		keys.queues[i] = fmt.Sprintf("%s:queue:%s", base, escapeKeyPart(queue))
	}
	return keys
}

func escapeKeyPart(value string) string {
	return url.PathEscape(value)
}

func invalidConfig(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfig, fmt.Sprintf(format, args...))
}
