package pubsub

import (
	"context"
	"time"
)

type NoopProvider struct{}

func (np NoopProvider) Publish(ctx context.Context, topic string, msg interface{}, json bool) error {
	return nil
}

func (np NoopProvider) Subscribe(topic, subscriberName string, h MsgHandler, deadline time.Duration, autoAck bool) {
	return
}
