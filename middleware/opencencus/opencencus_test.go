package opencensus

import (
	"context"
	"testing"

	gw "github.com/grpc-ecosystem/grpc-gateway/examples/examplepb"
	"github.com/lileio/pubsub"
	"github.com/lileio/pubsub/memory"
	"github.com/stretchr/testify/assert"
)

type TestSubscriber struct {
	JSON bool
	T    *testing.T
}

func (ts *TestSubscriber) DoSomething(ctx context.Context, t *gw.ABitOfEverything, msg *pubsub.Msg) error {
	assert.True(ts.T, len(msg.Data) > 0)
	return nil
}

func (ts *TestSubscriber) Setup(c *pubsub.Client) {
	c.On(pubsub.HandlerOptions{
		Topic:   "test_topic",
		Name:    "do_something",
		Handler: ts.DoSomething,
		JSON:    ts.JSON,
	})
}

func TestOpencencusMiddleware(t *testing.T) {
	m1 := Middleware()

	m := &memory.MemoryProvider{}
	c := &pubsub.Client{
		ServiceName:          "test",
		Provider:             m,
		SubscriberMiddleware: []pubsub.SubscriberMiddleware{m1},
	}

	ps := gw.ABitOfEverything{
		StringValue: "strprefix/foo",
	}

	err := c.Publish(context.Background(), "test_topic", &ps, false)
	assert.Nil(t, err)

	ts := TestSubscriber{T: t}
	ts.Setup(c)
}
