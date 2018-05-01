package pubsubzap

import (
	"context"
	"time"

	"github.com/lileio/pubsub"
	opentracing "github.com/opentracing/opentracing-go"
	zipkintracing "github.com/openzipkin/zipkin-go-opentracing"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// DefaultLogger creates the default, minimal setup if a no logger is passed
// to the middleware when setup
func DefaultLogger() *zap.Logger {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, _ := config.Build()
	return logger
}

// Middleware is middleware for zap logging
type Middleware struct {
	Logger *zap.Logger
}

// SubscribeInterceptor returns a subscriber middleware with added logging via Zap
func (o Middleware) SubscribeInterceptor(opts pubsub.HandlerOptions, next pubsub.MsgHandler) pubsub.MsgHandler {
	return func(ctx context.Context, m pubsub.Msg) error {
		if o.Logger == nil {
			o.Logger = DefaultLogger()
		}

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

		fields := []zapcore.Field{
			zap.String("component", "pubsub"),
			zap.String("topic", opts.Topic),
			zap.String("handler", opts.Name),
			zap.Duration("duration", elapsed),
			zap.Error(err),
		}

		if m.ID != "" {
			fields = append(fields, zap.String("id", m.ID))
		}

		if traceID != "" {
			fields = append(fields, zap.String("trace-id", traceID))
		}

		o.Logger.Debug("Processed PubSub Msg",
			fields...,
		)

		return err
	}
}

// PublisherMsgInterceptor add logging to the publisher
func (o Middleware) PublisherMsgInterceptor(serviceName string, next pubsub.PublishHandler) pubsub.PublishHandler {
	return func(ctx context.Context, topic string, m *pubsub.Msg) error {
		if o.Logger == nil {
			o.Logger = DefaultLogger()
		}

		start := time.Now()
		err := next(ctx, topic, m)
		elapsed := time.Now().Sub(start)

		o.Logger.Debug("Published PubSub Msg",
			zap.String("component", "pubsub"),
			zap.String("topic", topic),
			zap.Duration("duration", elapsed),
			zap.Error(err),
		)

		return err
	}
}
