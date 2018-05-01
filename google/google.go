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
	client *pubsub.Client
	topics map[string]*pubsub.Topic
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
func (g *GoogleCloud) Publish(ctx context.Context, topic string, m *ps.Msg) error {
	t, err := g.getTopic(ctx, topic)
	if err != nil {
		return err
	}

	res := t.Publish(ctx, &pubsub.Message{
		Data:       m.Data,
		Attributes: m.Metadata,
	})

	_, err = res.Get(context.Background())
	return err
}

// Subscribe implements Subscribe
func (g *GoogleCloud) Subscribe(opts ps.HandlerOptions, h ps.MsgHandler) {
	g.subscribe(opts, h, make(chan bool, 1))
}

func (g *GoogleCloud) subscribe(opts ps.HandlerOptions, h ps.MsgHandler, ready chan<- bool) {
	go func() {
		var err error
		subName := opts.ServiceName + "." + opts.Name + "--" + opts.Topic
		sub := g.client.Subscription(subName)

		t, err := g.getTopic(context.Background(), opts.Topic)
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
				AckDeadline: opts.Deadline,
			}
			sub, err = g.client.CreateSubscription(context.Background(), subName, sc)
			if err != nil {
				logrus.Panicf("Can't subscribe to topic: %s", err.Error())
			}
		}

		logrus.Infof("Subscribed to topic %s with name %s", opts.Topic, subName)
		ready <- true

		b := &backoff.Backoff{
			//These are the defaults
			Min:    200 * time.Millisecond,
			Max:    600 * time.Second,
			Factor: 2,
			Jitter: true,
		}

		sub.ReceiveSettings = pubsub.ReceiveSettings{MaxOutstandingMessages: opts.Concurrency}

		// Listen to messages and call the MsgHandler
		for {
			err = sub.Receive(context.Background(), func(ctx context.Context, m *pubsub.Message) {
				b.Reset()
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
					return
				}

				if opts.AutoAck {
					m.Ack()
				}
			})

			if err != nil {
				d := b.Duration()
				logrus.Errorf(
					"Subscription receive to topic %s failed, reconnecting in %v. Err: %v",
					opts.Topic, d, err,
				)
				time.Sleep(d)
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
