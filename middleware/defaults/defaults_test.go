package defaults

import (
	"context"
	"errors"
	"testing"

	"github.com/lileio/logr"
	"github.com/lileio/pubsub/v2"
	"github.com/lileio/pubsub/v2/providers/memory"
	"github.com/lileio/pubsub/v2/test"
	"github.com/stretchr/testify/assert"
)

type TestSubscriber struct {
	JSON bool
	T    *testing.T
}

func (ts *TestSubscriber) DoSomething(ctx context.Context, t *test.Account, msg *pubsub.Msg) error {
	assert.True(ts.T, len(msg.Data) > 0)
	panic(errors.New("ahhhh"))
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

func TestDefaultMiddleware(t *testing.T) {
	logr.SetLevelFromEnv()
	m := &memory.MemoryProvider{ErrorHandler: func(err error) {}}
	c := &pubsub.Client{
		ServiceName: "test",
		Provider:    m,
		Middleware:  Middleware,
	}

	ps := test.Account{
		Name: "pubsub",
	}

	err := c.Publish(context.Background(), "test_topic", &ps, false)
	assert.Nil(t, err)

	ts := TestSubscriber{T: t}
	ts.Setup(c)
}
