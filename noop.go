package pubsub

import (
	"context"
	"time"
)

// NoopProvider is a simple provider that does nothing, for testing, defaults
type NoopProvider struct{}

// Publish does nothing
func (np NoopProvider) Publish(ctx context.Context, topic string, b []byte) error {
	return nil
}

// Subscribe does nothing
func (np NoopProvider) Subscribe(topic, subscriberName string, h MsgHandler, deadline time.Duration, autoAck bool) {
	return
}
