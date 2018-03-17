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

// Middleware returns a subscriber middleware with added logging via Zap
func Middleware(logger *zap.Logger) pubsub.SubscriberMiddleware {
	if logger == nil {
		logger = DefaultLogger()
	}

	return func(opts pubsub.HandlerOptions, next pubsub.MsgHandler) pubsub.MsgHandler {
		return func(ctx context.Context, m pubsub.Msg) error {
			start := time.Now()
			err := next(ctx, m)
			elapsed := time.Now().Sub(start)

			logger.Info("Processed PubSub Msg",
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
}
