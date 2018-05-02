package recover

import (
	"context"

	"github.com/dropbox/godropbox/errors"
	"github.com/lileio/pubsub"
)

// Middleware is middleware for recovering from panics
type Middleware struct{}

// SubscribeInterceptor returns a subscriber middleware with added logging via Zap
func (o Middleware) SubscribeInterceptor(opts pubsub.HandlerOptions, next pubsub.MsgHandler) pubsub.MsgHandler {
	return func(ctx context.Context, m pubsub.Msg) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = errors.Wrap(err, "subscribe panic")
			}
		}()
		err = next(ctx, m)
		return
	}
}

// PublisherMsgInterceptor adds recovery to the publisher
func (o Middleware) PublisherMsgInterceptor(serviceName string, next pubsub.PublishHandler) pubsub.PublishHandler {
	return func(ctx context.Context, topic string, m *pubsub.Msg) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = errors.Wrap(err, "publish panic")
			}
		}()
		err = next(ctx, topic, m)
		return
	}
}
