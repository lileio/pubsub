package google

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	raw "cloud.google.com/go/pubsub/apiv1"
	"github.com/dropbox/godropbox/errors"
	"github.com/golang/protobuf/ptypes"
	"github.com/jpillora/backoff"
	"github.com/lileio/logr"
	ps "github.com/lileio/pubsub"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	pb "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"

	"github.com/sirupsen/logrus"
)

const endPoint = "pubsub.googleapis.com:443"

var (
	mutex = &sync.Mutex{}
)

// GoogleCloud provides google cloud pubsub
type GoogleCloud struct {
	init       sync.Once
	client     *pubsub.Client
	grpcClient *raw.SubscriberClient
	grpcErr    error
	topics     map[string]*pubsub.Topic
	projectID  string
	shutdown   bool
}

// NewGoogleCloud creates a new GoogleCloud instace for a project
func NewGoogleCloud(projectID string) (*GoogleCloud, error) {
	c, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, err
	}

	return &GoogleCloud{
		client:    c,
		projectID: projectID,
		topics:    map[string]*pubsub.Topic{},
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

	// var wg sync.WaitGroup
	// for k, v := range g.subs {
	// 	wg.Add(1)
	// 	logrus.Infof("Shutting down sub for %s", k)
	// 	go func(c context.CancelFunc) {
	// 		c()
	// 		wg.Done()
	// 	}(v)
	// }
	// wg.Wait()
	return
}

func (g *GoogleCloud) subscribe(opts ps.HandlerOptions, h ps.MsgHandler, ready chan<- bool) {
	go func() {
		var err error
		subName := opts.ServiceName + "." + opts.Name + "--" + opts.Topic
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
			Min:    200 * time.Millisecond,
			Max:    600 * time.Second,
			Factor: 2,
			Jitter: true,
		}

		// Listen to messages and call the MsgHandler
		for {
			if g.shutdown {
				break
			}

			req := &pb.PullRequest{
				Subscription: fmt.Sprintf("projects/%s/subscriptions/%s", g.projectID, subName),
				MaxMessages:  int32(opts.Concurrency),
			}

			conn, err := g.defaultConn(context.Background())
			if err != nil {
				break
			}

			resp, err := conn.Pull(context.Background(), req)
			if err != nil {
				d := b.Duration()
				logrus.Errorf(
					"pull from topic %s failed, retrying in %v. Err: %v",
					opts.Topic, d, err,
				)
				time.Sleep(d)
				continue
			}

			logr.WithCtx(context.Background()).Debugf(
				"Google Pubsub: pulled %d messages for %s",
				len(resp.ReceivedMessages),
				subName,
			)

			b.Reset()

			var wg sync.WaitGroup
			for _, m := range resp.ReceivedMessages {
				go func() {
					wg.Add(1)
					defer wg.Done()
					done := make(chan struct{})

					cctx, cancel := context.WithTimeout(context.Background(), opts.Deadline)
					defer cancel()

					go func(m *pb.ReceivedMessage) {
						defer func() {
							done <- struct{}{}
						}()

						t, _ := ptypes.Timestamp(m.Message.PublishTime)
						msg := ps.Msg{
							ID:          m.Message.MessageId,
							Metadata:    m.Message.Attributes,
							Data:        m.Message.Data,
							PublishTime: &t,
							Ack: func() {
								g.AckMessage(cctx, subName, []string{m.AckId})
							},
							Nack: func() {
								g.ModAckMessage(cctx, subName, []string{m.AckId}, 0)
							},
						}

						err = h(cctx, msg)
						if err != nil {
							timeout := 5 * time.Minute
							g.ModAckMessage(cctx, subName, []string{m.AckId}, int32(timeout))
							return
						}

						if opts.AutoAck {
							g.AckMessage(cctx, subName, []string{m.AckId})
						}
					}(m)

					select {
					case <-done:
					case <-time.After(opts.Deadline):
						logr.WithCtx(cctx).Debugf("Google Pubsub: worker timed out %v", subName)
					}
				}()
			}

			wg.Wait()
		}
	}()
}

func (g *GoogleCloud) AckMessage(ctx context.Context, sub string, ackIDs []string) error {
	req := &pb.AcknowledgeRequest{
		Subscription: sub,
		AckIds:       ackIDs,
	}

	conn, err := g.defaultConn(context.Background())
	if err != nil {
		return err
	}

	return conn.Acknowledge(context.Background(), req)
}

func (g *GoogleCloud) ModAckMessage(ctx context.Context, sub string, ackIDs []string, deadline int32) error {
	req := &pb.ModifyAckDeadlineRequest{
		Subscription:       sub,
		AckIds:             ackIDs,
		AckDeadlineSeconds: deadline,
	}

	conn, err := g.defaultConn(context.Background())
	if err != nil {
		return err
	}

	return conn.ModifyAckDeadline(context.Background(), req)
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

func (g *GoogleCloud) defaultConn(ctx context.Context) (*raw.SubscriberClient, error) {
	g.init.Do(func() {
		creds, err := defaultCredentials(ctx)
		if err != nil {
			g.grpcErr = err
			return
		}

		conn, _, err := dial(ctx, creds.TokenSource)
		if err != nil {
			g.grpcErr = err
			return
		}

		c, err := raw.NewSubscriberClient(ctx, option.WithGRPCConn(conn))
		g.grpcClient = c
		return

	})

	return g.grpcClient, g.grpcErr
}

func defaultCredentials(ctx context.Context) (*google.Credentials, error) {
	adc, err := google.FindDefaultCredentials(ctx,
		"https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, err
	}
	return adc, nil
}

func dial(ctx context.Context, ts oauth2.TokenSource) (*grpc.ClientConn, func(), error) {
	conn, err := grpc.DialContext(ctx, endPoint,
		grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")),
		grpc.WithPerRPCCredentials(oauth.TokenSource{TokenSource: ts}),
		grpc.WithUserAgent("lile-pubsub"),
	)
	if err != nil {
		return nil, nil, err
	}

	return conn, func() { conn.Close() }, nil
}
