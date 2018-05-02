package pubsub_test

import (
	"context"
	"testing"

	"github.com/lileio/pubsub"
	"github.com/lileio/pubsub/memory"
	"github.com/lileio/pubsub/test"
	"github.com/stretchr/testify/assert"
)

type TestSubscriber struct {
	JSON bool
	T    *testing.T
}

func (ts *TestSubscriber) DoSomething(ctx context.Context, t *test.Account, msg *pubsub.Msg) error {
	if ts.T != nil {
		assert.True(ts.T, len(msg.Data) > 0)
	}

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

func TestProtoSubscribers(t *testing.T) {
	m := &memory.MemoryProvider{}
	c := &pubsub.Client{
		ServiceName: "test",
		Provider:    m,
	}

	ps := test.Account{Name: "test"}

	for i := 0; i < 100; i++ {
		err := c.Publish(context.Background(), "test_topic", &ps, false)
		assert.Nil(t, err)
	}

	ts := TestSubscriber{T: t}
	ts.Setup(c)
}

func TestJSONSubscribers(t *testing.T) {
	m := &memory.MemoryProvider{}
	c := &pubsub.Client{
		ServiceName: "test",
		Provider:    m,
	}

	ps := test.Account{Name: "test"}

	for i := 0; i < 100; i++ {
		err := c.Publish(context.Background(), "test_topic", &ps, true)
		assert.Nil(t, err)
	}

	ts := TestSubscriber{T: t, JSON: true}
	ts.Setup(c)
}

func BenchmarkProtoSubscribers(b *testing.B) {
	m := &memory.MemoryProvider{}
	c := &pubsub.Client{
		ServiceName: "test",
		Provider:    m,
	}

	ps := test.Account{Name: "test"}

	for i := 0; i < b.N; i++ {
		c.Publish(context.Background(), "test_topic", &ps, false)
	}

	b.ResetTimer()
	ts := TestSubscriber{}
	ts.Setup(c)
}

func BenchmarkJSONSubscribers(b *testing.B) {
	m := &memory.MemoryProvider{}
	c := &pubsub.Client{
		ServiceName: "test",
		Provider:    m,
	}

	ps := test.Account{Name: "test"}

	for i := 0; i < b.N; i++ {
		c.Publish(context.Background(), "test_topic", &ps, true)
	}

	b.ResetTimer()
	ts := TestSubscriber{JSON: true}
	ts.Setup(c)
}
