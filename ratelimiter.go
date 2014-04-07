package ratelimiter

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

type RateLimiterBackend interface {
	GetState(string, time.Time) (LimitState, error)
	SetLimit(string, int64)
	ResetResource(string)
	Reserve(string, time.Time)
	Check()
}

type LimitState struct {
	RPILimit      int64
	ConsumedSlots int64
	LastReserved  time.Time
	FirstReserved time.Time
}

type RateLimiter struct {
	Interval time.Duration
	backend  RateLimiterBackend
	base     int64
}

// Create new RateLimiter
//  server: host:port of server
//  password: "" if auth is not required
func NewRateLimiter(server string, password string) *RateLimiter {
	interval, _ := time.ParseDuration("1s")
	backend := NewRedisBackend(server, password, interval)
	return &RateLimiter{
		backend:  backend,
		Interval: interval,
		base:     time.Now().UnixNano(),
	}
}

func (self *RateLimiter) Check() {
	self.backend.Check()
}

func (self *RateLimiter) Consume(resource string, timeout time.Duration) (ok bool, err error) {
	now := time.Now()
	state, err := self.backend.GetState(resource, now)
	if err != nil {
		panic(err)
	}
	if (state.ConsumedSlots < state.RPILimit) && (state.LastReserved.Before(now)) {
		self.backend.Reserve(resource, now)
		return true, nil
	} else {
		if state.LastReserved.After(now.Add(timeout)) {
			return false, nil
		}
		availabilityTime := state.FirstReserved.Add(self.Interval)
		if availabilityTime.After(now.Add(timeout)) {
			return false, nil
		} else {
			self.backend.Reserve(resource, availabilityTime)
			time.Sleep(availabilityTime.Sub(time.Now()))
			return true, nil
		}
	}
}

func (self *RateLimiter) SetLimit(resource string, rps int64) {
	self.backend.SetLimit(resource, rps)
}

func (self *RateLimiter) ResetResource(resource string) {
	self.backend.ResetResource(resource)
}

// Backend: redis
type RedisBackend struct {
	redisPool      *redis.Pool
	Interval       time.Duration
	ExpirationTime time.Duration
}

func (self *RedisBackend) Check() {
	c := self.redisPool.Get()
	_, err := c.Do("PING")
	if err != nil {
		panic(err)
	}
}

func (self *RedisBackend) SetLimit(resource string, rps int64) {
	self.redisPool.Get().Do("SET", fmt.Sprintf("rl.rpslim.%s", resource), rps)
}

func (self *RedisBackend) ResetResource(resource string) {
	self.redisPool.Get().Do("ZREMRANGEBYRANK", fmt.Sprintf("rl.reqs.%s", resource), 0, -1)
}

func (self *RedisBackend) Reserve(resource string, slot time.Time) {
	c := self.redisPool.Get()
	t := slot.UnixNano()
	expire := slot.Add(self.ExpirationTime).UnixNano()
	if t > expire {
		expire = t
	}
	expire = time.Unix(0, expire).Unix()
	key := fmt.Sprintf("rl.reqs.%s", resource)
	c.Send("ZADD", key, t, t)
	c.Send("EXPIREAT", key, expire)
	c.Flush()
	c.Receive()
	c.Receive()
}

func (self *RedisBackend) GetState(resource string, now time.Time) (LimitState, error) {
	c := self.redisPool.Get()
	key := fmt.Sprintf("rl.reqs.%s", resource)
	trim_time := now.Add(-self.Interval)
	c.Send("GET", fmt.Sprintf("rl.rpslim.%s", resource))
	c.Flush()
	c.Send("ZREMRANGEBYSCORE", key, "-inf", trim_time.UnixNano())
	c.Send("ZCOUNT", key, "-inf", "+inf")
	c.Send("ZRANGE", key, -1, -1)
	c.Flush()
	rps_limit, err := redis.Int64(c.Receive())
	if err != nil {
		return LimitState{}, errors.New("no limit set for given key")
	}
	rpi_limit := int64(self.Interval.Seconds() * float64(rps_limit))
	c.Send("ZRANGE", key, -rpi_limit, -rpi_limit)
	c.Flush()
	c.Receive()
	consumed_slots, err := redis.Int64(c.Receive())
	_last := redisInts64(c.Receive())
	var last int64 = 0
	if len(_last) > 0 {
		last = _last[0]
	}
	_first := redisInts64(c.Receive())
	var first int64 = 0
	if len(_first) > 0 {
		first = _first[0]
	}
	return LimitState{
		RPILimit:      rpi_limit,
		ConsumedSlots: consumed_slots,
		LastReserved:  time.Unix(0, last),
		FirstReserved: time.Unix(0, first),
	}, nil
}

func NewRedisBackend(server string, password string, interval time.Duration) *RedisBackend {
	expirationTime, _ := time.ParseDuration("1000s")
	return &RedisBackend{
		redisPool: &redis.Pool{
			MaxIdle:     10000,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", server)
				if err != nil {
					return nil, err
				}
				if password != "" {
					if _, err := c.Do("AUTH", password); err != nil {
						c.Close()
						return nil, err
					}
				}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
		Interval:       interval,
		ExpirationTime: expirationTime,
	}
}

//

func redisInts64(raw interface{}, err error) []int64 {
	if err != nil {
		panic(err)
	}
	data := raw.([]interface{})
	result := make([]int64, len(data))
	for i := range data {
		result[i], _ = redis.Int64(data[i], err)
	}
	return result
}
