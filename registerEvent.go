package delayTask

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"strings"
	"time"
)

func (s *Service) RegisterEvent(name string, args []string, delay int64) error {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "opId", strings.ReplaceAll(uuid.New().String(), "-", ""))

	lg := CtxLogger(ctx).WithField("name", name)

	s.mx.RLock()
	_, ok := s.slot[name]
	s.mx.RUnlock()

	if !ok {
		err := fmt.Errorf("event: %s func is unregister", name)
		return err
	}

	E := &event{
		Name: name,
		Args: args,
		Id:   strings.ReplaceAll(uuid.New().String(), "-", ""),
	}

	EB, err := json.Marshal(E)
	if err != nil {
		lg.Errorf("marshal event failed: %v", err)
		return err
	}

	score := int64(time.Now().Unix()) + delay
	err = s.redis.ZAdd(ctx, s.key, redis.Z{Score: float64(score), Member: string(EB)}).Err()
	if err != nil {
		lg.WithError(err).Error("register event failed")
		return err
	}

	return nil
}

func (s *Service) RegisterEventFunc(name string, f func(p []string) error) error {
	s.mx.RLock()
	_, ok := s.slot[name]
	s.mx.RUnlock()
	if ok {
		panic("event already exists")
	}

	s.mx.Lock()
	s.slot[name] = f
	s.mx.Unlock()

	return nil
}
