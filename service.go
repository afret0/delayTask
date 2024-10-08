package delayTask

import (
	"fmt"
	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"sync"
)

var RetryErr = fmt.Errorf("retry")

type Service struct {
	redis    redis.UniversalClient
	caller   string
	slot     map[string]func(p string) error
	mx       sync.RWMutex
	key      string
	UnAckKey string
	init     bool
	lock     *redislock.Client
	debug    bool
}

type event struct {
	Id              string `json:"id"`
	Name            string `json:"name"`
	Args            string `json:"args"`
	RetryCount      int64  `json:"retryCount"`
	UnAckRetryCount int64  `json:"unAckRetryCount"`
}

func NewService(caller string, redis redis.UniversalClient) *Service {
	if caller == "" {
		panic("caller is required")
	}

	svr := &Service{
		redis:    redis,
		caller:   caller,
		slot:     make(map[string]func(p string) error),
		mx:       sync.RWMutex{},
		key:      fmt.Sprintf("%s:delayTask", caller),
		UnAckKey: fmt.Sprintf("%s:delayTask:unAck", caller),
		init:     true,
		lock:     redislock.New(redis),
	}

	go svr.startTick()

	return svr
}

func (s *Service) SetDebug(debug bool) {
	s.debug = debug
}
