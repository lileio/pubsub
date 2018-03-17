package opencensus

import (
	"context"
	"log"
	"time"

	"github.com/lileio/pubsub"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

// Distribution views
var (
	DefaultBytesDistribution        = view.DistributionAggregation{0, 1024, 2048, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824, 4294967296}
	DefaultMillisecondsDistribution = view.DistributionAggregation{0, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 5000, 10000, 20000, 50000, 100000}
	DefaultMessageCountDistribution = view.DistributionAggregation{0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}
)

// The keys for metrics
var (
	KeyTopic, _   = tag.NewKey("topic")
	KeyHandler, _ = tag.NewKey("handler")
)

// The following variables are measures recorded by the subscriber
var (
	PubSubErrorCount, _   = stats.Int64("pubsub/subscriber/error_count", "Pubsub Errors", stats.UnitNone)
	PubSubElapsedTime, _  = stats.Float64("pubsub/subscriber/elapsed_time", "Subscriber elapsed time in msecs", stats.UnitMilliseconds)
	PubSubHandledBytes, _ = stats.Int64("pubsub/subscriber/bytes", "Handled bytes", stats.UnitBytes)
	PubSubHandledCount, _ = stats.Int64("pubsub/subscriber/handled_count", "Number of server subscribers handled", stats.UnitNone)
)

// The views for collecting metrics
var (
	PubSubErrorCountView = &view.View{
		Name:        "pubsub/subscriber/error_count",
		Description: "Pubsub Errors",
		TagKeys:     []tag.Key{KeyTopic, KeyHandler},
		Measure:     PubSubErrorCount,
		Aggregation: view.CountAggregation{},
	}

	PubSubElapsedTimeView = &view.View{
		Name:        "pubsub/subscriber/elapsed_time",
		Description: "Subscriber elapsed time in msecs",
		TagKeys:     []tag.Key{KeyTopic, KeyHandler},
		Measure:     PubSubElapsedTime,
		Aggregation: DefaultMillisecondsDistribution,
	}

	PubSubHandledBytesView = &view.View{
		Name:        "pubsub/subscriber/bytes",
		Description: "Handled bytes",
		TagKeys:     []tag.Key{KeyTopic, KeyHandler},
		Measure:     PubSubHandledBytes,
		Aggregation: DefaultBytesDistribution,
	}

	PubSubHandledCountView = &view.View{
		Name:        "pubsub/subscriber/handled_count",
		Description: "Number of server subscribers handled",
		TagKeys:     []tag.Key{KeyTopic, KeyHandler},
		Measure:     PubSubHandledCount,
		Aggregation: DefaultMessageCountDistribution,
	}
)

// All default views provided by this package:
var (
	DefaultViews = []*view.View{
		PubSubErrorCountView,
		PubSubElapsedTimeView,
		PubSubHandledBytesView,
		PubSubHandledCountView,
	}
)

// Middleware returns a subscriber middleware with tracing and stats to
// opencensus
func Middleware() pubsub.SubscriberMiddleware {
	if err := view.Subscribe(DefaultViews...); err != nil {
		log.Fatal(err)
	}

	return func(opts pubsub.HandlerOptions, next pubsub.MsgHandler) pubsub.MsgHandler {
		return func(ctx context.Context, m pubsub.Msg) error {
			mods := []tag.Mutator{
				tag.Upsert(KeyHandler, opts.Name),
				tag.Upsert(KeyTopic, opts.Topic),
			}

			ms := []stats.Measurement{
				PubSubHandledCount.M(1),
				PubSubHandledBytes.M(int64(len(m.Data))),
			}

			ctx, span := trace.StartSpan(ctx, opts.Name)
			defer span.End()

			startTime := time.Now()
			err := next(ctx, m)
			if err != nil {
				ms = append(ms, PubSubErrorCount.M(1))
			}

			elapsedTime := time.Since(startTime)
			ms = append(ms, PubSubElapsedTime.M(float64(elapsedTime)/float64(time.Millisecond)))

			ctx, _ = tag.New(ctx, mods...)
			stats.Record(ctx, ms...)
			return err
		}
	}
}
