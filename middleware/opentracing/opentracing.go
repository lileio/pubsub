package opentracing

import (
	"context"

	"github.com/lileio/pubsub"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

var (
	pubsubTag = opentracing.Tag{
		Key:   string(ext.Component),
		Value: "pubsub",
	}
)

type consumerOption struct {
	clientContext opentracing.SpanContext
}

func (c consumerOption) Apply(o *opentracing.StartSpanOptions) {
	if c.clientContext != nil {
		opentracing.ChildOf(c.clientContext).Apply(o)
	}
	ext.SpanKindConsumer.Apply(o)
}

// Middleware returns a subscriber middleware with added logging via Zap
func Middleware(tracer opentracing.Tracer) pubsub.SubscriberMiddleware {
	if tracer == nil {
		tracer = opentracing.GlobalTracer()
	}

	return func(opts pubsub.HandlerOptions, next pubsub.MsgHandler) pubsub.MsgHandler {
		return func(ctx context.Context, m pubsub.Msg) error {
			spanContext, err := tracer.Extract(
				opentracing.TextMap,
				opentracing.TextMapCarrier(m.Metadata))

			if err == nil {
				handlerSpan := tracer.StartSpan(
					opts.Name,
					consumerOption{clientContext: spanContext},
					pubsubTag,
				)
				defer handlerSpan.Finish()
				ctx = opentracing.ContextWithSpan(ctx, handlerSpan)
			}

			err = next(ctx, m)
			return err
		}
	}
}
