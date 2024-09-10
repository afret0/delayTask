package delayTask

import (
	"fmt"
	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"sync"
)

var svr *Service

type Service struct {
	redis  redis.UniversalClient
	caller string
	slot   map[string]func(p []string) error
	mx     sync.RWMutex
	key    string
	init   bool
	lock   *redislock.Client
}

type Event struct {
	Id   string   `json:"id"`
	Name string   `json:"name"`
	Args []string `json:"args"`
}

func GetService() *Service {
	if svr == nil {
		panic("service is not initialized")
	}

	if !svr.init {
		panic("service is not initialized")
	}

	return svr
}

func InitService(caller string, redis redis.UniversalClient) *Service {
	if caller == "" {
		panic("caller is required")
	}

	if svr != nil {
		return svr
	}

	svr = &Service{
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
