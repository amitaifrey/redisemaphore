package redisemaphore_test

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type redisCommandHook struct {
	process func(context.Context, redis.Cmder, redis.ProcessHook) error
}

func (redisCommandHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (hook redisCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		return hook.process(ctx, cmd, next)
	}
}

func (redisCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
