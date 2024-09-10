package delayTask

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"testing"
	"time"
)

func EF(args []string) error {
	fmt.Printf("args: %v\n", args)
	return nil
}

func TestService(t *testing.T) {
	ctx := context.Background()

	RC := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    []string{"120.27.235.209:6379"},
		Password: "Qiyiguo2303",
		Username: "default",
	})

	InitService("test", RC)

	svr1 := GetService()
	for i := 11; i <= 21; i++ {
		event := fmt.Sprintf("test:event:%d", i)
		err := svr1.RegisterEventFunc(ctx, event, EF)
		if err != nil {
			t.Error(err)
		}

		err = svr1.RegisterEvent(ctx, event, []string{"arg1", "arg2", "arg3"}, int64(i))
		if err != nil {
			t.Error(err)
		}
	}

	time.Sleep(100 * time.Second)

}
