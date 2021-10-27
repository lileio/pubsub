package opentelemetry

import (
	"context"
	"testing"

	"github.com/lileio/pubsub/v2"
	"github.com/lileio/pubsub/v2/providers/memory"
	"github.com/lileio/pubsub/v2/test"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/baggage"
)

type TestSubscriber struct {
	JSON bool
	T    *testing.T
}

func (ts *TestSubscriber) DoSomething(ctx context.Context, t *test.Account, msg *pubsub.Msg) error {
	b := baggage.FromContext(ctx)
	au := b.Member("audit-user")
	assert.NotNil(ts.T, au.Value())
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

func TestOpentelemetryMiddleware(t *testing.T) {
	m0, _ := baggage.NewMember(string("audit-user"), "acc_"+ksuid.New().String())
	b, _ := baggage.New(m0)
	ctx := baggage.ContextWithBaggage(context.Background(), b)

	m1 := Middleware{}
	m := &memory.MemoryProvider{}
	c := &pubsub.Client{
		ServiceName: "test",
		Provider:    m,
		Middleware:  []pubsub.Middleware{m1},
	}

	ps := test.Account{
		Name: "pubsub",
	}

	err := c.Publish(ctx, "test_topic", &ps, false)
	assert.Nil(t, err)

	ts := TestSubscriber{T: t}
	ts.Setup(c)
}
