package delayTask

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"keke/infrastructure/tool"
	"strings"
	"time"
)

func (s *Service) startTick() {
	logger := GetLogger()
	defer func() { logger.Infof("tick stop...") }()

	for range time.Tick(1 * time.Second) {
		logger.Infof("tick..")

		now := time.Now().Unix()
		eventL, err := s.redis.ZRangeByScore(context.Background(), s.key, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", now)}).Result()
		if err != nil {
			logger.Errorf("get event failed: %v", err)
			continue
		}

		for _, v := range eventL {
			ctx := context.Background()
			ctx = context.WithValue(ctx, "opId", strings.ReplaceAll(uuid.New().String(), "-", ""))
			lg := CtxLogger(ctx)

			v := v

			go s.runEvent(ctx, v)

			err = s.redis.ZRem(ctx, s.key, v).Err()
			if err != nil {
				lg.Errorf("remove event failed: %v", err)
				continue
			}
		}

	}
}

func (s *Service) runEvent(ctx context.Context, eventS string) {
	lg := CtxLogger(ctx).WithField("event", eventS)
	lg.Infof("event run...")
	defer lg.Infof("event end...")

	E := new(Event)
	err := json.Unmarshal([]byte(eventS), E)
	if err != nil {
		lg.WithError(err).Errorf("unmarshal event failed, event: %s", eventS)
		return
	}

	s.mx.RLock()
	f, ok := s.slot[E.Name]
	s.mx.RUnlock()

	if !ok {
		err := fmt.Errorf("event: %s func is unregister", E.Name)
		lg.Error(err)
		return
	}

	LT := 24 * time.Hour
	if !tool.IsProEnv() {
		LT = 3 * time.Second
	}
	_, err = s.lock.Obtain(ctx, fmt.Sprintf("%s:delayTask:lock:%s", s.caller, eventS), LT, nil)
	if err != nil {
		if errors.Is(err, redislock.ErrNotObtained) {
			lg.Infof("未获取到锁, 判断为重复执行")
			return
		}
		lg.Errorf("obtain lock failed, 不执行, err: %s", err)
		return
	}
	//defer func() { _ = lk.Release(ctx) }()

	err = f(E.Args)
	if err != nil {
		lg.Errorf("run event failed, err: %v", err)
		return
	}
}
