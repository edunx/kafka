package kafka

import (
	"context"
	"golang.org/x/time/rate"
)

type Limiter struct {
	limit     *rate.Limiter
	ctx       context.Context
}

func (lt *Limiter) Handler( name string , id int) {
	if lt.limit == nil {
		return
	}

	err := lt.limit.Wait(lt.ctx)
	if err != nil {
		Out.Err("%s thread.id=%d limit wait err: %v", name, id, err)
		return
	}
}
