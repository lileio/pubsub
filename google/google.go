package google

import (
	"context"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	ps "github.com/lileio/pubsub"
	ctxNet "golang.org/x/net/context"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/sirupsen/logrus"
)

var (
	mutex     = &sync.Mutex{}
	pubsubTag = opentracing.Tag{string(ext.Component), "pubsub"}
)

type GoogleCloud struct {
	client *pubsub.Client
	topics map[string]*pubsub.Topic
}

type consumerOption struct {
	clientContext opentracing.SpanContext
}

func (c consumerOption) Apply(o *opentracing.StartSpanOptions) {
	if c.clientContext != nil {
		opentracing.ChildOf(c.clientContext).Apply(o)
	}
	ext.SpanKindConsumer.Apply(o)
}

func NewGoogleCloud(project_id string) (*GoogleCloud, error) {
	c, err := pubsub.NewClient(ctxNet.Background(), project_id)
	if err != nil {
		return nil, err
	}

	return &GoogleCloud{
		client: c,
		topics: map[string]*pubsub.Topic{},
	}, nil
}

func (g *GoogleCloud) Publish(ctx context.Context, topic string, b []byte) error {
	tracer := opentracing.GlobalTracer()
	span := spanFromContext(ctx, tracer, topic)
	defer span.Finish()

	span.LogEvent("get topic")
	t, err := g.getTopic(topic)
	span.LogEvent("topic received")
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

	_, err = res.Get(ctxNet.Background())
	span.LogEvent("publish confirmed")
	return err
}

func (g *GoogleCloud) Subscribe(topic, subscriberName string, h ps.MsgHandler, deadline time.Duration, autoAck bool) {
	g.subscribe(topic, subscriberName, h, deadline, autoAck, make(chan bool, 1))
}

func (g *GoogleCloud) subscribe(topic, subscriberName string, h ps.MsgHandler, deadline time.Duration, autoAck bool, ready chan<- bool) {
	go func() {
		var sub *pubsub.Subscription
		var err error

		// Subscribe with backoff for failure (i.e topic doesn't exist yet)
		for {
			t, err := g.getTopic(topic)
			if err != nil {
				logrus.Errorf("Can't fetch topic: %s", err.Error())
				continue
			}

			subName := subscriberName + "--" + topic
			sc := pubsub.SubscriptionConfig{
				Topic:       t,
				AckDeadline: deadline,
			}

			sub, err = g.client.CreateSubscription(ctxNet.Background(), subName, sc)
			if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
				logrus.Errorf("Can't subscribe to topic: %s", err.Error())
				continue
			}

			logrus.Infof("Subscribed to topic %s with name %s", topic, subName)
			break
		}

		ready <- true

		// Listen to messages and call the MsgHandler
		for {
			err = sub.Receive(ctxNet.Background(), func(ctx ctxNet.Context, m *pubsub.Message) {
				logrus.Infof("Recevied on topic %s, id: %s", topic, m.ID)

				tracer := opentracing.GlobalTracer()
				spanContext, err := tracer.Extract(
					opentracing.TextMap,
					opentracing.TextMapCarrier(m.Attributes))
				if err != nil {
					logrus.Error(err)
					return
				}

				handlerSpan := tracer.StartSpan(
					subscriberName,
					consumerOption{clientContext: spanContext},
					pubsubTag,
				)
				defer handlerSpan.Finish()
				ctx = opentracing.ContextWithSpan(ctx, handlerSpan)

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
			}
		}
	}()
}

func (g *GoogleCloud) getTopic(name string) (*pubsub.Topic, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if g.topics[name] != nil {
		return g.topics[name], nil
	}

	ctx := ctxNet.Background()
	t, err := g.client.CreateTopic(ctx, name)
	if err != nil && !strings.Contains(err.Error(), "exists") {
		return nil, err
	}

	g.topics[name] = t

	return t, nil
}

func (g *GoogleCloud) deleteTopic(name string) error {
	t, err := g.getTopic(name)
	if err != nil {
		return err
	}

	return t.Delete(context.Background())
}

func spanFromContext(ctx context.Context, tracer opentracing.Tracer, name string) opentracing.Span {
	var parentCtx opentracing.SpanContext
	if parent := opentracing.SpanFromContext(ctx); parent != nil {
		parentCtx = parent.Context()
	}

	return tracer.StartSpan(
		name,
		opentracing.ChildOf(parentCtx),
		ext.SpanKindProducer,
		pubsubTag,
	)
}
