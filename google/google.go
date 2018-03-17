package google

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/jpillora/backoff"
	ps "github.com/lileio/pubsub"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/sirupsen/logrus"
)

var (
	mutex = &sync.Mutex{}
)

// GoogleCloud provides google cloud pubsub
type GoogleCloud struct {
	client          *pubsub.Client
	topics          map[string]*pubsub.Topic
	ReceiveSettings pubsub.ReceiveSettings
}

// NewGoogleCloud creates a new GoogleCloud instace for a project
func NewGoogleCloud(projectID string) (*GoogleCloud, error) {
	c, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, err
	}

	return &GoogleCloud{
		client: c,
		topics: map[string]*pubsub.Topic{},
	}, nil
}

// Publish implements Publish
func (g *GoogleCloud) Publish(ctx context.Context, topic string, b []byte) error {
	tracer := opentracing.GlobalTracer()
	span := spanFromContext(ctx, topic)
	defer span.Finish()

	t, err := g.getTopic(ctx, topic)
	if err != nil {
		return err
	}

	attrs := map[string]string{}
	tracer.Inject(
		span.Context(),
		opentracing.TextMap,
		opentracing.TextMapCarrier(attrs))

	span.LogEvent("publish")
	res := t.Publish(ctx, &pubsub.Message{
		Data:       b,
		Attributes: attrs,
	})

	_, err = res.Get(context.Background())
	span.LogEvent("publish confirmed")
	return err
}

// Subscribe implements Subscribe
func (g *GoogleCloud) Subscribe(topic, subscriberName string, h ps.MsgHandler, deadline time.Duration, autoAck bool) {
	g.subscribe(topic, subscriberName, h, deadline, autoAck, make(chan bool, 1))
}

func (g *GoogleCloud) subscribe(topic, subscriberName string, h ps.MsgHandler, deadline time.Duration, autoAck bool, ready chan<- bool) {
	go func() {
		var err error
		subName := subscriberName + "--" + topic
		sub := g.client.Subscription(subName)

		// Subscribe with backoff for failure (i.e topic doesn't exist yet)
		t, err := g.getTopic(context.Background(), topic)
		if err != nil {
			logrus.Panicf("Can't fetch topic: %s", err.Error())
		}

		ok, err := sub.Exists(context.Background())
		if err != nil {
			logrus.Panicf("Can't connect to pubsub: %s", err.Error())
		}

		if !ok {
			sc := pubsub.SubscriptionConfig{
				Topic:       t,
				AckDeadline: deadline,
			}
			sub, err = g.client.CreateSubscription(context.Background(), subName, sc)
			if err != nil {
				logrus.Panicf("Can't subscribe to topic: %s", err.Error())
			}
		}

		sub.ReceiveSettings = g.ReceiveSettings

		logrus.Infof("Subscribed to topic %s with name %s", topic, subName)
		ready <- true

		b := &backoff.Backoff{
			//These are the defaults
			Min:    200 * time.Millisecond,
			Max:    600 * time.Second,
			Factor: 2,
			Jitter: true,
		}

		// Listen to messages and call the MsgHandler
		for {
			err = sub.Receive(context.Background(), func(ctx context.Context, m *pubsub.Message) {
				b.Reset()

				logrus.Infof("Recevied on topic %s, id: %s", topic, m.ID)
				msg := ps.Msg{
					ID:       m.ID,
					Metadata: m.Attributes,
					Data:     m.Data,
					Ack: func() {
						m.Ack()
					},
					Nack: func() {
						m.Nack()
					},
				}

				err = h(ctx, msg)
				if err != nil {
					logrus.Error(err)
					return
				}

				if autoAck {
					m.Ack()
				}
			})

			if err != nil {
				logrus.Error(err)
				time.Sleep(b.Duration())
			}
		}
	}()
}

func (g *GoogleCloud) getTopic(ctx context.Context, name string) (*pubsub.Topic, error) {
	mutex.Lock()
	defer mutex.Unlock()

	span := spanFromContext(ctx, "fetch topic")
	defer span.Finish()

	if g.topics[name] != nil {
		return g.topics[name], nil
	}

	var err error
	t := g.client.Topic(name)
	ok, err := t.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !ok {
		t, err = g.client.CreateTopic(ctx, name)
		if err != nil {
			return nil, err
		}

	}

	g.topics[name] = t

	return t, nil
}

func (g *GoogleCloud) deleteTopic(name string) error {
	t, err := g.getTopic(context.Background(), name)
	if err != nil {
		return err
	}

	return t.Delete(context.Background())
}

func spanFromContext(ctx context.Context, name string) opentracing.Span {
	var parentCtx opentracing.SpanContext
	if parent := opentracing.SpanFromContext(ctx); parent != nil {
		parentCtx = parent.Context()
	}

	return opentracing.GlobalTracer().StartSpan(
		name,
		opentracing.ChildOf(parentCtx),
		ext.SpanKindProducer,
	)
}
