package opentelemetry

import (
	"context"

	"github.com/lileio/pubsub/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	name = "pubsub"
)

type MetadataCarrier map[string]string

func (m MetadataCarrier) Get(key string) string {
	return m[key]
}

func (m MetadataCarrier) Set(key, value string) {
	m[key] = value
}

func (m MetadataCarrier) Keys() []string {
	keys := []string{}
	for k, _ := range m {
		keys = append(keys, k)
	}

	return keys
}

// Middleware provides opentracing for pubsub to persist traces
type Middleware struct {
}

// SubscribeInterceptor returns a subscriber middleware with opentracing
func (o Middleware) SubscribeInterceptor(opts pubsub.HandlerOptions, next pubsub.MsgHandler) pubsub.MsgHandler {
	return func(ctx context.Context, m pubsub.Msg) error {
		// First extract baggage
		//
		// This propagates user-defined baggage associated with a trace. The complete
		// specification is defined at https://w3c.github.io/baggage/
		prop := propagation.TextMapPropagator(propagation.Baggage{})
		ctx = prop.Extract(ctx, MetadataCarrier(m.Metadata))

		// Then extract standard context using specified propagator
		ctx = otel.GetTextMapPropagator().Extract(ctx, MetadataCarrier(m.Metadata))
		var span trace.Span
		ctx, span = otel.Tracer(name).Start(ctx, opts.Name)
		defer func() { span.End() }()

		err := next(ctx, m)
		if err != nil {
			span.RecordError(err)
		}

		return err
	}
}

// PublisherMsgInterceptor inserts opentracing headers into an outgoing msg
func (o Middleware) PublisherMsgInterceptor(serviceName string, next pubsub.PublishHandler) pubsub.PublishHandler {
	return func(ctx context.Context, topic string, m *pubsub.Msg) error {
		if m.Metadata == nil {
			m.Metadata = map[string]string{}
		}

		// First extract baggage
		//
		// This propagates user-defined baggage associated with a trace. The complete
		// specification is defined at https://w3c.github.io/baggage/
		prop := propagation.TextMapPropagator(propagation.Baggage{})
		prop.Inject(ctx, MetadataCarrier(m.Metadata))

		// Then extract standard context using specified propagator
		otel.GetTextMapPropagator().Inject(ctx, MetadataCarrier(m.Metadata))

		var span trace.Span
		ctx, span = otel.Tracer(name).Start(ctx, "publish")
		span.SetAttributes(label.String("topic", topic))

		defer func() { span.End() }()
		err := next(ctx, topic, m)
		if err != nil {
			span.RecordError(err)
		}

		return err
	}
}
