package pubsub

import (
	"context"
	"time"
)

type NoopProvider struct{}

func (np NoopProvider) Publish(ctx context.Context, topic string, b []byte) error {
	return nil
}

func (np NoopProvider) Subscribe(topic, subscriberName string, h MsgHandler, deadline time.Duration, autoAck bool) {
	return
}
