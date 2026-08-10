package redisemaphore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type semaphoreTokenState struct {
	token         string
	holderExpiry  int64
	hasHolder     bool
	waiterExpiry  int64
	hasWaiter     bool
	queueIndex    int
	hasQueue      bool
	requestID     string
	hasRequest    bool
	activeToken   string
	activeMatches bool
}

func optionalRedisString(value any) (string, bool, error) {
	if value == nil {
		return "", false, nil
	}
	switch value := value.(type) {
	case string:
		return value, true, nil
	case []byte:
		return string(value), true, nil
	default:
		return "", false, fmt.Errorf("%w: unexpected Redis string %T", errInvalidSemaphoreState, value)
	}
}

func optionalHGet(ctx context.Context, tx *redis.Tx, key, field string) (string, bool, error) {
	value, err := tx.HGet(ctx, key, field).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func optionalZScore(ctx context.Context, tx *redis.Tx, key, member string) (int64, bool, error) {
	value, err := tx.ZScore(ctx, key, member).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	valueMillis, err := redisScoreMillis(value)
	return valueMillis, true, err
}

func redisScoreMillis(value float64) (int64, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > math.MaxInt64 || math.Trunc(value) != value {
		return 0, fmt.Errorf("%w: invalid millisecond score %v", errInvalidSemaphoreState, value)
	}
	return int64(value), nil
}

func loadTokenStates(ctx context.Context, tx *redis.Tx, keys semaphoreKeys, tokens []string) (map[string]semaphoreTokenState, error) {
	unique := make([]string, 0, len(tokens))
	seen := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		if _, exists := seen[token]; exists {
			continue
		}
		seen[token] = struct{}{}
		unique = append(unique, token)
	}
	states := make(map[string]semaphoreTokenState, len(unique))
	if len(unique) == 0 {
		return states, nil
	}

	holderCommands := make([]*redis.FloatCmd, len(unique))
	waiterCommands := make([]*redis.FloatCmd, len(unique))
	var queueValues, requestValues *redis.SliceCmd
	_, err := tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for index, token := range unique {
			holderCommands[index] = pipe.ZScore(ctx, keys.holders, token)
			waiterCommands[index] = pipe.ZScore(ctx, keys.waiters, token)
		}
		queueValues = pipe.HMGet(ctx, keys.tokenQueue, unique...)
		requestValues = pipe.HMGet(ctx, keys.tokenRequest, unique...)
		return nil
	})
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	queues, err := queueValues.Result()
	if err != nil {
		return nil, err
	}
	requests, err := requestValues.Result()
	if err != nil {
		return nil, err
	}
	if len(queues) != len(unique) || len(requests) != len(unique) {
		return nil, fmt.Errorf("%w: token metadata result length mismatch", errInvalidSemaphoreState)
	}

	requestFields := make([]string, 0, len(unique))
	requestSeen := make(map[string]struct{}, len(unique))
	for index, token := range unique {
		state := semaphoreTokenState{token: token}
		holderScore, holderErr := holderCommands[index].Result()
		if holderErr == nil {
			state.holderExpiry, err = redisScoreMillis(holderScore)
			if err != nil {
				return nil, err
			}
			state.hasHolder = true
		} else if !errors.Is(holderErr, redis.Nil) {
			return nil, holderErr
		}
		waiterScore, waiterErr := waiterCommands[index].Result()
		if waiterErr == nil {
			state.waiterExpiry, err = redisScoreMillis(waiterScore)
			if err != nil {
				return nil, err
			}
			state.hasWaiter = true
		} else if !errors.Is(waiterErr, redis.Nil) {
			return nil, waiterErr
		}
		queueText, hasQueue, queueErr := optionalRedisString(queues[index])
		if queueErr != nil {
			return nil, queueErr
		}
		if hasQueue {
			parsed, parseErr := strconv.Atoi(queueText)
			if parseErr != nil || parsed < 1 || parsed > len(keys.queues) {
				return nil, fmt.Errorf("%w: invalid queue index %q for token", errInvalidSemaphoreState, queueText)
			}
			state.queueIndex = parsed
			state.hasQueue = true
		}
		state.requestID, state.hasRequest, err = optionalRedisString(requests[index])
		if err != nil {
			return nil, err
		}
		if state.hasRequest {
			if _, exists := requestSeen[state.requestID]; !exists {
				requestSeen[state.requestID] = struct{}{}
				requestFields = append(requestFields, state.requestID)
			}
		}
		states[token] = state
	}

	activeByRequest := make(map[string]string, len(requestFields))
	if len(requestFields) != 0 {
		values, readErr := tx.HMGet(ctx, keys.activeRequest, requestFields...).Result()
		if readErr != nil {
			return nil, readErr
		}
		if len(values) != len(requestFields) {
			return nil, fmt.Errorf("%w: active request result length mismatch", errInvalidSemaphoreState)
		}
		for index, requestID := range requestFields {
			active, exists, parseErr := optionalRedisString(values[index])
			if parseErr != nil {
				return nil, parseErr
			}
			if exists {
				activeByRequest[requestID] = active
			}
		}
	}
	for token, state := range states {
		if state.hasRequest {
			state.activeToken = activeByRequest[state.requestID]
			state.activeMatches = state.activeToken == token
			states[token] = state
		}
	}
	return states, nil
}

type queueCandidate struct {
	token  string
	expiry int64
}

func readQueueCandidates(ctx context.Context, tx *redis.Tx, keys semaphoreKeys, limit int64) ([][]queueCandidate, error) {
	rangeCommands := make([]*redis.StringSliceCmd, len(keys.queues))
	_, err := tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for index, key := range keys.queues {
			rangeCommands[index] = pipe.ZRange(ctx, key, 0, limit-1)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	tokensByQueue := make([][]string, len(keys.queues))
	scoreCommands := make([]*redis.FloatSliceCmd, len(keys.queues))
	_, err = tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for index, command := range rangeCommands {
			tokens, resultErr := command.Result()
			if resultErr != nil {
				return resultErr
			}
			tokensByQueue[index] = tokens
			if len(tokens) != 0 {
				scoreCommands[index] = pipe.ZMScore(ctx, keys.waiters, tokens...)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	result := make([][]queueCandidate, len(keys.queues))
	for queueIndex, tokens := range tokensByQueue {
		if len(tokens) == 0 {
			continue
		}
		scores, scoreErr := scoreCommands[queueIndex].Result()
		if scoreErr != nil {
			return nil, scoreErr
		}
		if len(scores) != len(tokens) {
			return nil, fmt.Errorf("%w: queue score result length mismatch", errInvalidSemaphoreState)
		}
		result[queueIndex] = make([]queueCandidate, len(tokens))
		for index, token := range tokens {
			expiry := int64(0)
			if scores[index] != 0 {
				expiry, err = redisScoreMillis(scores[index])
				if err != nil {
					return nil, err
				}
			}
			result[queueIndex][index] = queueCandidate{token: token, expiry: expiry}
		}
	}
	return result, nil
}

type tokenRemoval struct {
	state       semaphoreTokenState
	extraQueues map[int]struct{}
}

func addTokenRemoval(removals map[string]*tokenRemoval, state semaphoreTokenState, extraQueue int) {
	removal := removals[state.token]
	if removal == nil {
		removal = &tokenRemoval{state: state, extraQueues: make(map[int]struct{})}
		removals[state.token] = removal
	}
	if extraQueue >= 1 {
		removal.extraQueues[extraQueue] = struct{}{}
	}
}

func queueTokenRemovals(ctx context.Context, pipe redis.Pipeliner, keys semaphoreKeys, removals map[string]*tokenRemoval) {
	for token, removal := range removals {
		pipe.ZRem(ctx, keys.holders, token)
		pipe.ZRem(ctx, keys.waiters, token)
		if removal.state.hasQueue {
			pipe.ZRem(ctx, keys.queues[removal.state.queueIndex-1], token)
		}
		for queueIndex := range removal.extraQueues {
			if !removal.state.hasQueue || queueIndex != removal.state.queueIndex {
				pipe.ZRem(ctx, keys.queues[queueIndex-1], token)
			}
		}
		if removal.state.hasRequest && removal.state.activeMatches {
			pipe.HDel(ctx, keys.activeRequest, removal.state.requestID)
		}
		pipe.HDel(ctx, keys.waiterStarted, token)
		pipe.HDel(ctx, keys.tokenQueue, token)
		pipe.HDel(ctx, keys.tokenRequest, token)
	}
}
