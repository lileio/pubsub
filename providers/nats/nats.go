package nats

import (
	"context"
	fmt "fmt"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/lileio/logr"
	"github.com/lileio/pubsub"
	stan "github.com/nats-io/stan.go"
	"github.com/pkg/errors"
	"github.com/segmentio/ksuid"
	"github.com/sirupsen/logrus"
)

var (
	mutex = &sync.Mutex{}
)

// Nats provides nats publish and subscribe
type Nats struct {
	client   stan.Conn
	clientID string
	topics   map[string]*stan.Subscription
	shutdown bool
}

// NewNats creates a new Nats connection for a project
func NewNats(clusterName string, opts ...stan.Option) (*Nats, error) {
	n := ksuid.New().String()

	nc, err := stan.Connect(clusterName, n, opts...)
	if err != nil {
		return nil, err
	}

	return &Nats{
		client:   nc,
		clientID: n,
		topics:   map[string]*stan.Subscription{},
	}, nil
}

// Publish implements Publish
func (n *Nats) Publish(ctx context.Context, topic string, m *pubsub.Msg) error {
	w := &pubsub.MessageWrapper{
		Data:        m.Data,
		Metadata:    m.Metadata,
		PublishTime: ptypes.TimestampNow(),
	}

	b, err := proto.Marshal(w)
	if err != nil {
		return err
	}

	err = n.client.Publish(topic, b)
	if err != nil {
		return err
	}

	if err != nil {
		logr.WithCtx(ctx).Error(errors.Wrap(err, "couldn't publish to nats"))
	} else {
		logr.WithCtx(ctx).Debugf("Nats: Published to %s as %s", topic, n.clientID)
	}

	return err
}

// Subscribe implements Subscribe for Nats
func (n *Nats) Subscribe(opts pubsub.HandlerOptions, h pubsub.MsgHandler) {
	queueName := fmt.Sprintf("%s--%s", opts.ServiceName, opts.Name)
	sub, err := n.client.QueueSubscribe(opts.Topic, queueName, func(m *stan.Msg) {
		var w pubsub.MessageWrapper
		err := proto.Unmarshal(m.Data, &w)
		if err != nil {
			logr.WithCtx(context.Background()).Errorf(
				"Nats: [WARNING] Couldn't unmarshal msg from topic %s, reason: %s",
				opts.Topic,
				err,
			)
			return
		}

		t, err := ptypes.Timestamp(w.PublishTime)
		if err != nil {
			logr.WithCtx(context.Background()).Errorf(
				"Nats: [WARNING] Couldn't unmarshal timestamp topic %s, reason: %s",
				opts.Topic,
				err,
			)
			return
		}

		id := strconv.FormatUint(m.MsgProto.Sequence, 10)
		msg := pubsub.Msg{
			ID:          id,
			Metadata:    w.Metadata,
			Data:        w.Data,
			PublishTime: &t,
			Ack: func() {
				m.Ack()
			},
			Nack: func() {},
		}

		err = h(context.Background(), msg)
		if err != nil {
			return
		}

		if opts.AutoAck {
			m.Ack()
		}

	},
		stan.StartWithLastReceived(),
		stan.DurableName(queueName),
		stan.AckWait(opts.Deadline),
		stan.MaxInflight(opts.Concurrency),
		stan.SetManualAckMode(),
	)

	n.topics[opts.Topic] = &sub

	if err != nil {
		logr.WithCtx(context.Background()).Errorf(
			"Nats: [WARNING] Couldn't subscribe to %s, reason: %s",
			opts.Topic,
			err,
		)
	}
}

// Shutdown shuts down all subscribers gracefully
func (n *Nats) Shutdown() {
	n.client.Close()

	n.shutdown = true
	var wg sync.WaitGroup
	for k, v := range n.topics {
		logrus.Infof("Shutting down sub for %s", k)
		go func(v *stan.Subscription) {
			(*v).Close()
			wg.Done()
		}(v)
	}
	wg.Wait()
	return
}
