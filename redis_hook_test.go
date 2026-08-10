package redisemaphore_test

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type redisCommandHook struct {
	process         func(context.Context, redis.Cmder, redis.ProcessHook) error
	processPipeline func(context.Context, []redis.Cmder, redis.ProcessPipelineHook) error
}

func (redisCommandHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (hook redisCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if hook.process == nil {
			return next(ctx, cmd)
		}
		return hook.process(ctx, cmd, next)
	}
}

func (hook redisCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if hook.processPipeline == nil {
			return next(ctx, cmds)
		}
		return hook.processPipeline(ctx, cmds, next)
	}
}
