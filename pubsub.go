//Package pubsub implements publish subscriber patterns for usage in Golang
//go:generate mockgen -source pubsub.go -destination pubsub_mock.go -package pubsub
package pubsub

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
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
		Buckets: prometheus.DefBuckets,
	})

	client = &Client{Provider: NoopProvider{}}
)

func init() {
	prometheus.MustRegister(publishedCounter)
	prometheus.MustRegister(publishedSize)
	prometheus.MustRegister(publishDurationsHistogram)
}

// Client holds a reference to a Provider
type Client struct {
	ServiceName string
	Provider    Provider
}

// Provider is generic interface for a pub sub provider
type Provider interface {
	Publish(ctx context.Context, topic string, msg proto.Message) error
	Subscribe(topic, subscriberName string, h MsgHandler, deadline time.Duration, autoAck bool)
}

// Subscriber is a service/service that listens to events and registers handlers
// for those events
type Subscriber interface {
	// Setup is a required method that allows the subscriber service to add handlers
	// and perform any setup if required, this is usually called by lile upon start
	Setup(*Client)
}

// Msg is a lile representation of a pub sub message
type Msg struct {
	ID       string
	Metadata map[string]string
	Data     []byte

	Ack  func()
	Nack func()
}

// Handler is a specific callback used for Subscribe in the format of..
// func(ctx context.Context, obj proto.Message, msg *Msg) error
// for example, you can unmarshal a custom type..
// func(ctx context.Context, accounts accounts.Account, msg *Msg) error
type Handler interface{}

// MsgHandler is the internal or raw message handler
type MsgHandler func(ctx context.Context, m Msg) error

// SetClient sets the global pubsub client, useful in tests
func SetClient(cli *Client) {
	client = cli
}

// Publish published on the client
func (c *Client) Publish(ctx context.Context, topic string, msg proto.Message) error {
	start := time.Now()
	err := c.Provider.Publish(ctx, topic, msg)
	if err != nil {
		return err
	}

	publishedCounter.WithLabelValues(topic, client.ServiceName).Inc()
	publishedSize.WithLabelValues(topic, client.ServiceName).Add(float64(len([]byte(msg.String()))))
	publishDurationsHistogram.Observe(time.Now().Sub(start).Seconds())
	return nil
}

// Publish is a convenience message which publishes to the current (global) publisher
func Publish(ctx context.Context, topic string, msg proto.Message) error {
	start := time.Now()
	err := client.Provider.Publish(ctx, topic, msg)
	if err != nil {
		return err
	}

	publishedCounter.WithLabelValues(topic, client.ServiceName).Inc()
	publishedSize.WithLabelValues(topic, client.ServiceName).Add(float64(len([]byte(msg.String()))))
	publishDurationsHistogram.Observe(time.Now().Sub(start).Seconds())
	return nil
}
