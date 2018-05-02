package pubsub

import (
	"context"
)

// NoopProvider is a simple provider that does nothing, for testing, defaults
type NoopProvider struct{}

// Publish does nothing
func (np NoopProvider) Publish(ctx context.Context, topic string, m *Msg) error {
	return nil
}

// Subscribe does nothing
func (np NoopProvider) Subscribe(opts HandlerOptions, h MsgHandler) {
	return
}

// Shutdown shutsdown immediately
func (np NoopProvider) Shutdown() {
	return
}
