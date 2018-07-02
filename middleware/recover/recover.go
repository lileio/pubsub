package recover

import (
	"context"

	"github.com/dropbox/godropbox/errors"
	"github.com/lileio/pubsub"
)

// Middleware is middleware for recovering from panics
type Middleware struct {
	RecoveryHandlerFunc RecoveryHandlerFunc
}

// RecoveryHandlerFunc is a function that recovers from the panic `p` by returning an `error`.
type RecoveryHandlerFunc func(p interface{}) (err error)

// SubscribeInterceptor returns a subscriber middleware with added logging via Zap
func (o Middleware) SubscribeInterceptor(opts pubsub.HandlerOptions, next pubsub.MsgHandler) pubsub.MsgHandler {
	return func(ctx context.Context, m pubsub.Msg) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = recoverFrom(r, "pubsub: subscriber panic \n", o.RecoveryHandlerFunc)
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
				err = recoverFrom(r, "pubsub: publish error", o.RecoveryHandlerFunc)
			}
		}()
		err = next(ctx, topic, m)
		return
	}
}

func recoverFrom(p interface{}, wrap string, r RecoveryHandlerFunc) error {
	if r == nil {
		return errors.Wrap(p.(error), wrap)
	}

	return r(p)
}
