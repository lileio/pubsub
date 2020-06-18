package google

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/dropbox/godropbox/errors"
	"github.com/jpillora/backoff"
	"github.com/lileio/logr"
	ps "github.com/lileio/pubsub/v2"
	"github.com/segmentio/ksuid"
	"golang.org/x/sync/semaphore"

	"github.com/sirupsen/logrus"
)

var (
	mutex = &sync.Mutex{}
)

// GoogleCloud provides google cloud pubsub
type GoogleCloud struct {
	client   *pubsub.Client
	topics   map[string]*pubsub.Topic
	subs     map[string]context.CancelFunc
	shutdown bool
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
		subs:   map[string]context.CancelFunc{},
	}, nil
}

// Publish implements Publish
func (g *GoogleCloud) Publish(ctx context.Context, topic string, m *ps.Msg) error {
	t, err := g.getTopic(topic)
	if err != nil {
		return err
	}

	logr.WithCtx(ctx).Debug("Google Pubsub: Publishing")
	res := t.Publish(context.Background(), &pubsub.Message{
		Data:       m.Data,
		Attributes: m.Metadata,
	})

	_, err = res.Get(context.Background())
	if err != nil {
		logr.WithCtx(ctx).Error(errors.Wrap(err, "publish get failed"))
	} else {
		logr.WithCtx(ctx).Debug("Google Pubsub: Publish confirmed")
	}

	return err
}

// Subscribe implements Subscribe
func (g *GoogleCloud) Subscribe(opts ps.HandlerOptions, h ps.MsgHandler) {
	g.subscribe(opts, h, make(chan bool, 1))
}

// Shutdown shuts down all subscribers gracefully
func (g *GoogleCloud) Shutdown() {
	g.shutdown = true

	var wg sync.WaitGroup
	for k, v := range g.subs {
		wg.Add(1)
		logrus.Infof("Shutting down sub for %s", k)
		go func(c context.CancelFunc) {
			c()
			wg.Done()
		}(v)
	}
	wg.Wait()
	return
}

func (g *GoogleCloud) subscribe(opts ps.HandlerOptions, h ps.MsgHandler, ready chan<- bool) {
	go func() {
		var err error
		subName := opts.ServiceName + "." + opts.Name + "--" + opts.Topic
		if opts.Unique {
			subName = subName + "-uniq-" + ksuid.New().String()
		}

		sub := g.client.Subscription(subName)

		t, err := g.getTopic(opts.Topic)
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

		// create a semaphore, this is because Google PubSub will spam
		// your service if you can't process a message
		// and will also not handle
		sem := semaphore.NewWeighted(int64(opts.Concurrency))

		// Listen to messages and call the MsgHandler
		for {
			if g.shutdown {
				if opts.Unique {
					err = sub.Delete(context.Background())
					if err != nil {
						logrus.Errorf(
							"pubsub: Failed to delete unique queue %s: %v",
							subName, err,
						)
					}

				}
				break
			}

			cctx, cancel := context.WithCancel(context.Background())
			err = sub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
				if serr := sem.Acquire(ctx, 1); serr != nil {
					logrus.Errorf(
						"pubsub: Failed to acquire worker semaphore: %v",
						serr,
					)
					return
				}
				defer sem.Release(1)

				b.Reset()
				msg := ps.Msg{
					ID:          m.ID,
					Metadata:    m.Attributes,
					Data:        m.Data,
					PublishTime: &m.PublishTime,
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

			g.subs[subName] = cancel
		}
	}()
}

func (g *GoogleCloud) getTopic(name string) (*pubsub.Topic, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if g.topics[name] != nil {
		return g.topics[name], nil
	}

	var err error
	t := g.client.Topic(name)
	ok, err := t.Exists(context.Background())
	if err != nil {
		return nil, err
	}

	if !ok {
		t, err = g.client.CreateTopic(context.Background(), name)
		if err != nil {
			return nil, err
		}
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
