package pubsub

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
)

type NoopProvider struct{}

func (np NoopProvider) Publish(ctx context.Context, topic string, msg proto.Message) error {
	return nil
}

func (np NoopProvider) Subscribe(topic, subscriberName string, h MsgHandler, deadline time.Duration, autoAck bool) {
	return
}
