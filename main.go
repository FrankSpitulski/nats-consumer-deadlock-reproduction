package main

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

const (
	streamName   = "test-stream"
	consumerName = streamName + "_consumer"
	messageCount = 100
)

func main() {
	development, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(development)
	ctx := context.Background()

	err := setup(ctx)
	if err != nil {
		zap.L().Fatal("setup", zap.Error(err))
	}
	err = publish(ctx)
	if err != nil {
		zap.L().Fatal("publish", zap.Error(err))
	}

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		zap.L().Fatal("Failed to connect to NATS", zap.Error(err))
	}
	consumer, err := NewNatsConsumer(ctx, nc)
	if err != nil {
		zap.L().Fatal("Failed to create NATS consumer", zap.Error(err))
	}
	for i := 0; i < messageCount; i++ {
		zap.L().Debug("consuming message", zap.Int("index", i))
		next, err := consumer.Next(ctx)
		if err != nil {
			zap.L().Fatal("Failed to get next message", zap.Error(err))
		}
		err = next.Ack()
		if err != nil {
			zap.L().Fatal("Failed to ack message", zap.Error(err))
		}
		zap.L().Debug("consumed message", zap.Int("index", i))
	}
}

func publish(ctx context.Context) error {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	for i := 0; i < messageCount; i++ {
		_, err = js.Publish(ctx, streamName+"."+uuid.New().String(), []byte("hello world"))
		if err != nil {
			return err
		}
	}
	zap.L().Info("Published messages", zap.Int("count", messageCount))
	return nil
}

func setup(ctx context.Context) error {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		zap.L().Fatal("Failed to connect to NATS", zap.Error(err))
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	err = js.DeleteStream(ctx, streamName)
	if err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		return err
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{streamName + ".>"},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.MemoryStorage,
	})
	if err != nil {
		return err
	}
	_, err = js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Name:       consumerName,
		Durable:    consumerName,
		MaxDeliver: 5,
	})
	if err != nil {
		return err
	}
	return nil
}
