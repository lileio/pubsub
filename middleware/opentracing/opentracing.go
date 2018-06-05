package opentracing

import (
	"context"

	"github.com/lileio/pubsub"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
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

// Middleware provides opentracing for pubsub to persist traces
type Middleware struct {
	Tracer opentracing.Tracer
}

// SubscribeInterceptor returns a subscriber middleware with opentracing
func (o Middleware) SubscribeInterceptor(opts pubsub.HandlerOptions, next pubsub.MsgHandler) pubsub.MsgHandler {
	return func(ctx context.Context, m pubsub.Msg) error {
		var tracer = o.Tracer
		if tracer == nil {
			tracer = opentracing.GlobalTracer()
		}

		spanContext, err := tracer.Extract(
			opentracing.TextMap,
			opentracing.TextMapCarrier(m.Metadata))

		if err != nil && err != opentracing.ErrSpanContextNotFound {
			return next(ctx, m)
		}

		handlerSpan := tracer.StartSpan(
			opts.Name,
			consumerOption{clientContext: spanContext},
			pubsubTag,
		)
		defer handlerSpan.Finish()

		ctx = opentracing.ContextWithSpan(ctx, handlerSpan)
		err = next(ctx, m)

		if err != nil {
			handlerSpan.SetTag("error", "true")
			handlerSpan.LogFields(log.String("err", err.Error()))
		}

		return err
	}
}

// PublisherMsgInterceptor inserts opentracing headers into an outgoing msg
func (o Middleware) PublisherMsgInterceptor(serviceName string, next pubsub.PublishHandler) pubsub.PublishHandler {
	return func(ctx context.Context, topic string, m *pubsub.Msg) error {
		var tracer = o.Tracer
		if tracer == nil {
			tracer = opentracing.GlobalTracer()
		}

		span := spanFromContext(ctx, tracer, topic)
		defer span.Finish()

		if m.Metadata == nil {
			m.Metadata = map[string]string{}
		}

		tracer.Inject(
			span.Context(),
			opentracing.TextMap,
			opentracing.TextMapCarrier(m.Metadata))

		err := next(ctx, topic, m)
		if err != nil {
			span.SetTag("error", "true")
			span.LogFields(log.String("err", err.Error()))
		}

		return err
	}
}

func spanFromContext(ctx context.Context, tracer opentracing.Tracer, name string) opentracing.Span {
	var parentCtx opentracing.SpanContext
	if parent := opentracing.SpanFromContext(ctx); parent != nil {
		parentCtx = parent.Context()
	}

	return tracer.StartSpan(
		name,
		opentracing.ChildOf(parentCtx),
		ext.SpanKindProducer,
		pubsubTag,
	)
}
