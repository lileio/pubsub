package logrus

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/dropbox/godropbox/errors"
	"github.com/lileio/pubsub/v2"
	opentracing "github.com/opentracing/opentracing-go"
	zipkintracing "github.com/openzipkin/zipkin-go-opentracing"
	"github.com/sirupsen/logrus"
)

// Middleware is middleware for logrus logging
type Middleware struct{}

// SubscribeInterceptor returns a subscriber middleware with added logging via Zap
func (o Middleware) SubscribeInterceptor(opts pubsub.HandlerOptions, next pubsub.MsgHandler) pubsub.MsgHandler {
	return func(ctx context.Context, m pubsub.Msg) error {
		start := time.Now()
		err := next(ctx, m)
		elapsed := time.Now().Sub(start)

		var traceID string
		span := opentracing.SpanFromContext(ctx)
		if span != nil {
			zs, ok := span.Context().(zipkintracing.SpanContext)
			if ok {
				traceID = zs.TraceID.ToHex()
			}
		}

		fields := logrus.Fields{
			"component": "pubsub",
			"topic":     opts.Topic,
			"handler":   opts.Name,
			"duration":  elapsed,
			"metadata":  m.Metadata,
		}

		if err != nil {
			de, ok := err.(errors.DropboxError)
			if ok {
				fields["err"] = de.GetMessage()
				fields["stack"] = de.GetStack()
			} else {
				fields["err"] = err
				fields["stack"] = string(debug.Stack())
			}
		}

		if m.ID != "" {
			fields["id"] = m.ID
		}

		if traceID != "" {
			fields["trace-id"] = traceID
		}

		logrus.WithFields(fields).Debug("Processed PubSub Msg")
		return err
	}
}

// PublisherMsgInterceptor adds logging to the publisher
func (o Middleware) PublisherMsgInterceptor(serviceName string, next pubsub.PublishHandler) pubsub.PublishHandler {
	return func(ctx context.Context, topic string, m *pubsub.Msg) error {
		start := time.Now()
		err := next(ctx, topic, m)
		elapsed := time.Now().Sub(start)

		fields := logrus.Fields{
			"component": "pubsub",
			"topic":     topic,
			"duration":  elapsed,
			"metadata":  m.Metadata,
		}

		if err != nil {
			de, ok := err.(errors.DropboxError)
			if ok {
				fields["err"] = de.GetMessage()
			} else {
				fields["err"] = err
			}
		}

		logrus.WithFields(fields).Debug("Published PubSub Msg")
		return err
	}
}
