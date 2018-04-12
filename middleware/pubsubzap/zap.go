package pubsubzap

import (
	"context"
	"time"

	"github.com/lileio/pubsub"
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

		o.Logger.Debug("Processed PubSub Msg",
			zap.String("component", "pubsub"),
			zap.String("id", m.ID),
			zap.String("topic", opts.Topic),
			zap.String("handler", opts.Name),
			zap.Duration("duration", elapsed),
			zap.Error(err),
		)
		return err
	}
}

// PublisherMsgInterceptor add logging to the publisher
func (o Middleware) PublisherMsgInterceptor(next pubsub.PublishHandler) pubsub.PublishHandler {
	return func(ctx context.Context, topic string, m *pubsub.Msg) error {
		if o.Logger == nil {
			o.Logger = DefaultLogger()
		}

		start := time.Now()
		err := next(ctx, topic, m)
		elapsed := time.Now().Sub(start)

		o.Logger.Debug("Processed PubSub Msg",
			zap.String("component", "pubsub"),
			zap.String("topic", topic),
			zap.Duration("duration", elapsed),
			zap.Error(err),
		)

		return err
	}
}
