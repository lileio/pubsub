package google

import (
	"context"
	fmt "fmt"
	math "math"
	"runtime"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	pbs "cloud.google.com/go/pubsub/apiv1"
	"github.com/dropbox/godropbox/errors"
	"github.com/golang/protobuf/ptypes"
	"github.com/jpillora/backoff"
	"github.com/lileio/logr"
	ps "github.com/lileio/pubsub/v2"
	"github.com/segmentio/ksuid"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/option"
	pbpb "google.golang.org/genproto/googleapis/pubsub/v1"

	"github.com/sirupsen/logrus"
)

var (
	mutex = &sync.Mutex{}
)

// GoogleCloud provides google cloud pubsub
type GoogleCloud struct {
	client    *pubsub.Client
	subClient *pbs.SubscriberClient

	projectID string

	topics map[string]*pubsub.Topic
	subs   map[string]context.CancelFunc

	shutdown bool
}

// NewGoogleCloud creates a new GoogleCloud instace for a project
func NewGoogleCloud(projectID string) (*GoogleCloud, error) {
	// Try to use as many connections as possible. Use the same maximum default as Google's library
	numConns := runtime.GOMAXPROCS(0)
	if numConns > 4 {
		numConns = 4
	}

	c, err := pubsub.NewClient(context.Background(), projectID, option.WithGRPCConnectionPool(numConns))
	if err != nil {
		return nil, err
	}

	s, err := pbs.NewSubscriberClient(context.Background(), option.WithGRPCConnectionPool(numConns))
	if err != nil {
		return nil, err
	}

	return &GoogleCloud{
		projectID: projectID,
		client:    c,
		subClient: s,
		topics:    map[string]*pubsub.Topic{},
		subs:      map[string]context.CancelFunc{},
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
}

type workUnit struct {
	ackID string
	m     *pbpb.PubsubMessage
	ctx   context.Context
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
			_, err = g.client.CreateSubscription(context.Background(), subName, sc)
			if err != nil {
				logrus.Panicf("Can't subscribe to topic: %s", err.Error())
			}
		} else {
			_, err = sub.Update(context.Background(), pubsub.SubscriptionConfigToUpdate{
				AckDeadline: opts.Deadline,
			})
			if err != nil {
				logrus.Panicf("Can't update: %s", err.Error())
			}
		}

		logrus.Infof("Subscribing to topic %s with name %s", opts.Topic, subName)
		ready <- true

		b := &backoff.Backoff{
			//These are the defaults
			Min:    200 * time.Millisecond,
			Max:    600 * time.Second,
			Factor: 2,
			Jitter: true,
		}

		// We use a semaphore as another flow control mechanism, so clients can decide
		// how many messages they want to process concurrently
		sem := semaphore.NewWeighted(int64(opts.Concurrency))
		workQueue := make(chan *workUnit)

		// Listen to messages and call the MsgHandler
		go func() {
			for w := range workQueue {
				ctx, c := context.WithTimeout(w.ctx, opts.Deadline)
				if serr := sem.Acquire(ctx, 1); serr != nil {
					logrus.Errorf(
						"pubsub: Failed to acquire worker semaphore: %v",
						serr,
					)
					c()
					continue
				}

				go func(w *workUnit, c context.CancelFunc) {
					defer sem.Release(1)
					defer c()

					m := w.m

					pt, _ := ptypes.Timestamp(m.PublishTime)

					msg := ps.Msg{
						ID:          m.MessageId,
						Metadata:    m.Attributes,
						Data:        m.Data,
						PublishTime: &pt,
						Ack: func() {
							req := &pbpb.AcknowledgeRequest{
								Subscription: "projects/" + g.projectID + "/subscriptions/" + subName,
								AckIds:       []string{w.ackID},
							}

							err := g.subClient.Acknowledge(context.Background(), req)
							if err != nil {
								logrus.Errorf(
									"Failed to Ack %s on sub %v. Err: %v",
									w.ackID, subName, err,
								)
							}
						},
						Nack: func() {
							req := &pbpb.ModifyAckDeadlineRequest{
								Subscription:       "projects/" + g.projectID + "/subscriptions/" + subName,
								AckIds:             []string{w.ackID},
								AckDeadlineSeconds: 300,
							}

							err := g.subClient.ModifyAckDeadline(context.Background(), req)
							if err != nil {
								logrus.Errorf(
									"Failed to modify deadline %s on sub %v. Err: %v",
									w.ackID, subName, err,
								)
							}
						},
					}

					err = h(ctx, msg)
					if err != nil {
						msg.Nack()
						return
					}

					if opts.AutoAck {
						msg.Ack()
					}
				}(w, c)

			}
		}()

		// Keep pulling messages from this subscription
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
				close(workQueue)
				break
			}

			ctx, cancel := context.WithCancel(context.Background())
			mutex.Lock()
			g.subs[subName] = cancel
			mutex.Unlock()

			if opts.Concurrency == 0 {
				opts.Concurrency = 10
			}

			req := &pbpb.PullRequest{
				Subscription: fmt.Sprintf("projects/%s/subscriptions/%s", g.projectID, subName),
				MaxMessages:  int32(math.Floor(float64(opts.Concurrency) * 1.2)),
			}

			res, err := g.subClient.Pull(ctx, req)
			if err != nil {
				d := b.Duration()
				logrus.Errorf(
					"Subscription pull from topic %s failed, retrying in %v. Err: %v",
					opts.Topic, d, err,
				)
				time.Sleep(d)
				_ = cancel
				continue
			}

			if len(res.ReceivedMessages) == 0 {
				time.Sleep(1 * time.Second)
				_ = cancel
				continue
			}

			for _, m := range res.ReceivedMessages {
				workQueue <- &workUnit{
					m:     m.Message,
					ackID: m.AckId,
					ctx:   ctx,
				}
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
