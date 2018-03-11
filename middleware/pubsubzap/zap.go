package pubsubzap

import (
	"context"
	"time"

	"github.com/lileio/pubsub"
	"go.uber.org/zap"
)

// DefaultLogger creates the default, minimal setup if a no logger is passed
// to the middleware when setup
func DefaultLogger() *zap.Logger {
	logger, _ := zap.NewDevelopment()
	return logger
}

// Middleware returns a subscriber middleware with added logging via Zap
func Middleware(logger *zap.Logger) pubsub.SubscriberMiddleware {
	if logger == nil {
		logger = DefaultLogger()
	}

	return func(opts pubsub.HandlerOptions, next pubsub.MsgHandler) pubsub.MsgHandler {
		return func(ctx context.Context, m pubsub.Msg) error {
			// if v := ctx.Value("test"); v != nil {
			// }

			start := time.Now()
			err := next(ctx, m)
			elapsed := time.Now().Sub(start)

			logger.Debug("Processed pubsub msg",
				zap.String("id", m.ID),
				zap.String("trace_id", "sometraceid"),
				zap.Duration("duration", elapsed),
				zap.Error(err),
			)
			return err
		}
	}
}
