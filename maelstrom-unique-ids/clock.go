package main

import (
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
}

type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now()
}

func (RealClock) Sleep(d time.Duration) {
	time.Sleep(d)
}

type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) Sleep(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func WaitNextMillis(c Clock, last int64) int64 {
	now := c.Now().UnixMilli()
	for now <= last {
		c.Sleep(time.Millisecond)
		now = c.Now().UnixMilli()
	}
	return now
}
