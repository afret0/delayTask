package delayTask

import (
	"fmt"
	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"sync"
)

type Service struct {
	redis  redis.UniversalClient
	caller string
	slot   map[string]func(p []string) error
	mx     sync.RWMutex
	key    string
	init   bool
	lock   *redislock.Client
}

type event struct {
	Id   string   `json:"id"`
	Name string   `json:"name"`
	Args []string `json:"args"`
}

func NewService(caller string, redis redis.UniversalClient) *Service {
	if caller == "" {
		panic("caller is required")
	}

	svr := &Service{
		redis:  redis,
		caller: caller,
		slot:   make(map[string]func(p []string) error),
		mx:     sync.RWMutex{},
		key:    fmt.Sprintf("%s:delayTask", caller),
		init:   true,
		lock:   redislock.New(redis),
	}

	go svr.startTick()

	return svr
}
