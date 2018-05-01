package prometheus

import (
	"context"
	"strconv"
	"time"

	"github.com/lileio/pubsub"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	publishedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pubsub_message_published_total",
			Help: "Total number of messages published by the client.",
		}, []string{"topic", "service"})

	publishedSize = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pubsub_outgoing_bytes",
			Help: "A counter of pubsub published outgoing bytes.",
		}, []string{"topic", "service"})

	publishDurationsHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "pubsub_publish_durations_histogram_seconds",
		Help:    "PubSub publishing latency distributions.",
		Buckets: prometheus.ExponentialBuckets(0.01, 1.5, 10),
	})

	pubsubHandled = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pubsub_server_handled_total",
			Help: "Total number of PubSubs handled on the server",
		}, []string{"topic", "service", "success"})

	subscriberSize = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pubsub_incoming_bytes",
			Help: "A counter of pubsub subscribers incoming bytes.",
		}, []string{"topic", "service"})

	subscribeDurationsHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pubsub_subscribe_durations_histogram_seconds",
		Help:    "PubSub subscriber latency distributions.",
		Buckets: prometheus.ExponentialBuckets(0.05, 1.5, 20),
	}, []string{"topic", "service"})
)

func init() {
	prometheus.MustRegister(publishedCounter)
	prometheus.MustRegister(publishedSize)
	prometheus.MustRegister(publishDurationsHistogram)
	prometheus.MustRegister(pubsubHandled)
	prometheus.MustRegister(subscriberSize)
	prometheus.MustRegister(subscribeDurationsHistogram)
}

// Middleware is middleware for prometheus metrics
type Middleware struct{}

// SubscribeInterceptor returns a subscriber middleware with added logging via Zap
func (o Middleware) SubscribeInterceptor(opts pubsub.HandlerOptions, next pubsub.MsgHandler) pubsub.MsgHandler {
	return func(ctx context.Context, m pubsub.Msg) error {
		start := time.Now()
		err := next(ctx, m)

		pubsubHandled.WithLabelValues(
			opts.Topic,
			opts.ServiceName,
			strconv.FormatBool(err == nil),
		).Inc()

		subscriberSize.WithLabelValues(
			opts.Topic,
			opts.ServiceName,
		).Add(float64(len(m.Data)))

		subscribeDurationsHistogram.WithLabelValues(
			opts.Topic,
			opts.ServiceName,
		).Observe(time.Now().Sub(start).Seconds())
		return err
	}
}

// PublisherMsgInterceptor add logging to the publisher
func (o Middleware) PublisherMsgInterceptor(serviceName string, next pubsub.PublishHandler) pubsub.PublishHandler {
	return func(ctx context.Context, topic string, m *pubsub.Msg) error {
		start := time.Now()
		err := next(ctx, topic, m)

		publishedCounter.
			WithLabelValues(topic, serviceName).Inc()
		publishDurationsHistogram.
			Observe(time.Now().Sub(start).Seconds())
		publishedSize.
			WithLabelValues(topic, serviceName).
			Add(float64(len(m.Data)))

		return err
	}
}
