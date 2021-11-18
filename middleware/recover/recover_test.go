package recover

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lileio/pubsub/v2"
	"github.com/lileio/pubsub/v2/providers/memory"
	"github.com/lileio/pubsub/v2/test"
	"github.com/stretchr/testify/assert"
)

type TestSubscriber struct {
	JSON bool
	T    *testing.T
}

func (ts *TestSubscriber) PanicWithError(ctx context.Context, t *test.Account, msg *pubsub.Msg) error {
	assert.True(ts.T, len(msg.Data) > 0)
	panic(errors.New("this is an error"))
}

func (ts *TestSubscriber) PanicWithString(ctx context.Context, t *test.Account, msg *pubsub.Msg) error {
	assert.True(ts.T, len(msg.Data) > 0)
	panic("this is a panic")
}

func (ts *TestSubscriber) PanicUnknown(ctx context.Context, t *test.Account, msg *pubsub.Msg) error {
	assert.True(ts.T, len(msg.Data) > 0)
	panic(struct{}{})
}

func (ts *TestSubscriber) Setup(c *pubsub.Client) {
	c.On(pubsub.HandlerOptions{
		Topic:   "with_error",
		Name:    "test",
		Handler: ts.PanicWithError,
		JSON:    ts.JSON,
	})

	c.On(pubsub.HandlerOptions{
		Topic:   "with_string",
		Name:    "test",
		Handler: ts.PanicWithString,
		JSON:    ts.JSON,
	})

	c.On(pubsub.HandlerOptions{
		Topic:   "with_unknown",
		Name:    "test",
		Handler: ts.PanicUnknown,
		JSON:    ts.JSON,
	})
}

func TestRecoverMiddleware(t *testing.T) {
	m1 := Middleware{}

	m := &memory.MemoryProvider{
		ErrorHandler: func(err error) {
			fmt.Printf("err = %+v\n", err)
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

	err := c.Publish(context.Background(), "with_error", &ps, false)
	assert.Nil(t, err)

	err = c.Publish(context.Background(), "with_string", &ps, false)
	assert.Nil(t, err)

	err = c.Publish(context.Background(), "with_unknown", &ps, false)
	assert.Nil(t, err)

	ts := TestSubscriber{T: t}
	ts.Setup(c)
}
