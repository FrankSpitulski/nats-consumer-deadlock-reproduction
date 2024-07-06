package main

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"time"
)

const (
	stop  = true
	drain = false
)

type GlobalNatsConsumer struct {
	consumer jetstream.Consumer
}

func NewNatsConsumer(ctx context.Context, nc *nats.Conn) (*GlobalNatsConsumer, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	consumer, err := js.Consumer(ctx, streamName, consumerName)
	if err != nil {
		return nil, err
	}
	return &GlobalNatsConsumer{consumer}, nil
}

func (c *GlobalNatsConsumer) Next(ctx context.Context) (jetstream.Msg, error) {
	// TODO place a breakpoint here to inspect the program while deadlocked
	debugCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	context.AfterFunc(debugCtx, func() {
		if errors.Is(debugCtx.Err(), context.DeadlineExceeded) {
			zap.L().Info("timeout breakpoint")
		}
	})

	messagesContext, err := c.consumer.Messages(jetstream.StopAfter(1), jetstream.PullMaxMessages(1))
	if err != nil {
		return nil, err
	}

	// call Drain() after the context ends so that we can break out of Next() and clean up
	releaseDrainAfterCtx := context.AfterFunc(ctx, func() {
		zap.L().Debug("draining as a result of context cancellation")
		if stop {
			messagesContext.Stop()
		}
		if drain {
			messagesContext.Drain()
		}
	})
	defer func() {
		// we have to unregister the Drain after func when releasing messagesContext,
		// else messagesContext will leak by staying associated with ctx.
		releaseDrainAfterCtx()
		zap.L().Debug("draining as a result of exit")
		if stop {
			messagesContext.Stop()
		}
		if drain {
			messagesContext.Drain()
		}
		if stop || drain {
			// unsubscribe and nack all buffered messages
			for {
				zap.L().Debug("calling next in drain")
				msg, err := messagesContext.Next()
				if err != nil {
					zap.L().Debug("finishing drain", zap.Error(err))
					return
				}
				zap.L().Debug("nacking drained msg")
				_ = msg.Nak()
			}
		}
	}()
	zap.L().Debug("calling next in fetch")
	return messagesContext.Next()
}
