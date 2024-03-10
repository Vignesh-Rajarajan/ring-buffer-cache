package cache

import "time"

type clock interface {
	epoch() int64
}
type realClock struct{}

func (c *realClock) epoch() int64 {
	return time.Now().Unix()
}
