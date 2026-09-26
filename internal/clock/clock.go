package clock

import (
	"sync/atomic"
	"time"
)

type Manual struct {
	ms atomic.Int64
}

func NewManual(start time.Time) *Manual {
	m := &Manual{}
	m.ms.Store(start.UnixMilli())
	return m
}

func (m *Manual) Now() time.Time {
	return time.UnixMilli(m.ms.Load())
}

func (m *Manual) Advance(d time.Duration) {
	m.ms.Add(d.Milliseconds())
}
