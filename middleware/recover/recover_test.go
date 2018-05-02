package recover

import (
	"context"
	"errors"
	"testing"

	"github.com/lileio/lile/test"
	"github.com/lileio/pubsub"
	"github.com/lileio/pubsub/memory"
	"github.com/stretchr/testify/assert"
)

type TestSubscriber struct {
	JSON bool
	T    *testing.T
}

func (ts *TestSubscriber) DoSomething(ctx context.Context, t *test.Account, msg *pubsub.Msg) error {
	assert.True(ts.T, len(msg.Data) > 0)
	panic(errors.New("ahhhhhhhh"))
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

func TestRecoverMiddleware(t *testing.T) {
	m1 := Middleware{}

	m := &memory.MemoryProvider{
		ErrorHandler: func(err error) {
			assert.NotNil(t, err)
		},
	}
	c := &pubsub.Client{
		ServiceName: "test",
		Provider:    m,
		Middleware:  []pubsub.Middleware{m1},
	}

	ps := test.Account{
		Name: "smth",
	}

	err := c.Publish(context.Background(), "test_topic", &ps, false)
	assert.Nil(t, err)

	ts := TestSubscriber{T: t}
	ts.Setup(c)
}
