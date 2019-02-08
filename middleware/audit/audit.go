package audit

import (
	"context"

	"github.com/lileio/pubsub"
	"google.golang.org/grpc/metadata"
)

// Middleware provides passing on the audit user for pubsub
type Middleware struct{}

// SubscribeInterceptor returns a subscriber middleware with context of an audit user
func (o Middleware) SubscribeInterceptor(opts pubsub.HandlerOptions, next pubsub.MsgHandler) pubsub.MsgHandler {
	return func(ctx context.Context, m pubsub.Msg) error {
		ctx = context.WithValue(ctx, "x-audit-user", m.Metadata["x-audit-user"])
		ctx = metadata.NewIncomingContext(ctx, metadata.New(m.Metadata))
		return next(ctx, m)
	}
}

// PublisherMsgInterceptor inserts audit user into pubsub metadata
func (o Middleware) PublisherMsgInterceptor(serviceName string, next pubsub.PublishHandler) pubsub.PublishHandler {
	return func(ctx context.Context, topic string, m *pubsub.Msg) error {
		var user string
		audit, ok := ctx.Value("x-audit-user").(string)
		if ok && audit != "" {
			user = audit
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			if len(md["x-audit-user"]) > 0 {
				user = md["x-audit-user"][0]
			}
		}

		m.Metadata["x-audit-user"] = user
		return next(ctx, topic, m)
	}
}
