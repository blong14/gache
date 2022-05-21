package limiter

import (
	"context"
	"sort"
	"time"

	"golang.org/x/time/rate"

	gerrors "github.com/blong14/gache/internal/errors"
)

type RateLimiter interface {
	Wait(context.Context) error
	Limit() rate.Limit
}

// implements RateLimiter
type multiLimiter struct {
	limiters []RateLimiter
}

func MultiLimiter(limiters ...RateLimiter) *multiLimiter {
	byLimit := func(i, j int) bool {
		return limiters[i].Limit() < limiters[j].Limit()
	}
	sort.Slice(limiters, byLimit)
	return &multiLimiter{limiters: limiters}
}

func (l *multiLimiter) Wait(ctx context.Context) error {
	for _, l := range l.limiters {
		if err := l.Wait(ctx); err != nil {
			return gerrors.NewGError(err)
		}
	}
	return nil
}

func (l *multiLimiter) Limit() rate.Limit {
	return l.limiters[0].Limit()
}

func Per(eventCount int, duration time.Duration) rate.Limit {
	return rate.Every(duration / time.Duration(eventCount))
}

func Burst(eventCount int) int {
	return eventCount
}
