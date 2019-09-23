package pubsubzap

import (
	"context"
	"testing"

	"github.com/lileio/pubsub"
	"github.com/lileio/pubsub/providers/memory"
	"github.com/lileio/pubsub/test"
	"github.com/stretchr/testify/assert"
)

type TestSubscriber struct {
	JSON bool
	T    *testing.T
}

func (ts *TestSubscriber) DoSomething(ctx context.Context, t *test.Account, msg *pubsub.Msg) error {
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

func TestZapMiddleware(t *testing.T) {
	m1 := Middleware{}

	m := &memory.MemoryProvider{}
	c := &pubsub.Client{
		ServiceName: "test",
		Provider:    m,
		Middleware:  []pubsub.Middleware{m1},
	}

	ps := test.Account{
		Name: "strprefix/foo",
	}

	err := c.Publish(context.Background(), "test_topic", &ps, false)
	assert.Nil(t, err)

	ts := TestSubscriber{T: t}
	ts.Setup(c)
}
