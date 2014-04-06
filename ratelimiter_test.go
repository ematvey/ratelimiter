package ratelimiter

import (
	// "fmt"
	"testing"
	"time"
)

type result struct {
	ok bool
	i  int
}

func TestConstructor(t *testing.T) {
	rl := NewRateLimiter("0.0.0.0:6379", "")
	rl.Check()
	rl.ResetResource("a")
	rl.SetLimit("a", 3)
	timeout, _ := time.ParseDuration("1s")
	result_chan := make(chan result, 10)

	sl, _ := time.ParseDuration("100ms")
	// sl2, _ := time.ParseDuration("5ms")

	for i := 0; i < 10; i++ {
		t.Logf("iteration i = %+v", i)
		time.Sleep(sl)
		go func(i int) {
			// time.Sleep(sl2)
			ok, err := rl.Consume("a", timeout)
			if err != nil {
				panic(err)
			}
			result_chan <- result{ok, i}
		}(i)
	}
	for j := 0; j < 10; j++ {
		result := <-result_chan
		t.Logf("result rcv: %+v\n", result)
		if (result.i <= 5 && !result.ok) || (result.i > 5 && result.ok) {
			t.Log(!(result.i <= 5 && result.ok), !(result.i > 5 && !result.ok))
			t.Fatalf("incorrect result, i %+v\n", result.i)
		}
	}

}
