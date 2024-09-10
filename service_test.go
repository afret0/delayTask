package delayTask

import (
	"context"
	"fmt"
	"keke/pkg/sample/infrastructure/sampleCache"
	"testing"
	"time"
)

func EF(args []string) error {
	fmt.Printf("args: %v\n", args)
	return nil
}

func TestService(t *testing.T) {
	ctx := context.Background()

	InitService("test", sampleCache.GetRedis())

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
