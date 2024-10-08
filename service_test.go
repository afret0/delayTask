package delayTask

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"testing"
	"time"
)

func EF(args string) error {
	fmt.Printf("args: %v\n", args)
	//panic("lsakdjlfkajlsd")
	time.Sleep(10 * time.Minute)
	//return nil
	return RetryErr
}

func TestService(t *testing.T) {
	//ctx := context.Background()

	RC := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    []string{"120.27.235.209:6379"},
		Password: "Qiyiguo2303",
		Username: "default",
	})

	//InitService("test", RC)

	svr1 := NewService("test", RC)
	for i := 11; i <= 21; i++ {
		event := fmt.Sprintf("test:event:%d", i)
		svr1.RegisterEventFunc(event, EF)

		err := svr1.RegisterEvent(event, "arg1", int64(i))
		if err != nil {
			t.Error(err)
		}
	}

	time.Sleep(100 * time.Second)

}
