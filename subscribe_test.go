package pubsub_test

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

func TestProtoSubscribers(t *testing.T) {
	m := &memory.MemoryProvider{}
	c := &pubsub.Client{
		ServiceName: "test",
		Provider:    m,
	}

	ps := gw.ABitOfEverything{
		FloatValue:               1.5,
		DoubleValue:              2.5,
		Int64Value:               4294967296,
		Uint64Value:              9223372036854775807,
		Int32Value:               -2147483648,
		Fixed64Value:             9223372036854775807,
		Fixed32Value:             4294967295,
		BoolValue:                true,
		StringValue:              "strprefix/foo",
		Uint32Value:              4294967295,
		Sfixed32Value:            2147483647,
		Sfixed64Value:            -4611686018427387904,
		Sint32Value:              2147483647,
		Sint64Value:              4611686018427387903,
		NonConventionalNameValue: "camelCase",
	}

	for i := 0; i < 100; i++ {
		err := c.Publish(context.Background(), "test_topic", &ps, false)
		assert.Nil(t, err)
	}

	ts := TestSubscriber{}
	ts.Setup(c)
}

func TestJSONSubscribers(t *testing.T) {
	m := &memory.MemoryProvider{}
	c := &pubsub.Client{
		ServiceName: "test",
		Provider:    m,
	}

	ps := gw.ABitOfEverything{
		FloatValue:               1.5,
		DoubleValue:              2.5,
		Int64Value:               4294967296,
		Uint64Value:              9223372036854775807,
		Int32Value:               -2147483648,
		Fixed64Value:             9223372036854775807,
		Fixed32Value:             4294967295,
		BoolValue:                true,
		StringValue:              "strprefix/foo",
		Uint32Value:              4294967295,
		Sfixed32Value:            2147483647,
		Sfixed64Value:            -4611686018427387904,
		Sint32Value:              2147483647,
		Sint64Value:              4611686018427387903,
		NonConventionalNameValue: "camelCase",
	}

	for i := 0; i < 100; i++ {
		err := c.Publish(context.Background(), "test_topic", &ps, true)
		assert.Nil(t, err)
	}

	ts := TestSubscriber{JSON: true}
	ts.Setup(c)
}

func BenchmarkProtoSubscribers(b *testing.B) {
	m := &memory.MemoryProvider{}
	c := &pubsub.Client{
		ServiceName: "test",
		Provider:    m,
	}

	ps := gw.ABitOfEverything{
		FloatValue:               1.5,
		DoubleValue:              2.5,
		Int64Value:               4294967296,
		Uint64Value:              9223372036854775807,
		Int32Value:               -2147483648,
		Fixed64Value:             9223372036854775807,
		Fixed32Value:             4294967295,
		BoolValue:                true,
		StringValue:              "strprefix/foo",
		Uint32Value:              4294967295,
		Sfixed32Value:            2147483647,
		Sfixed64Value:            -4611686018427387904,
		Sint32Value:              2147483647,
		Sint64Value:              4611686018427387903,
		NonConventionalNameValue: "camelCase",
	}

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

	ps := gw.ABitOfEverything{
		FloatValue:               1.5,
		DoubleValue:              2.5,
		Int64Value:               4294967296,
		Uint64Value:              9223372036854775807,
		Int32Value:               -2147483648,
		Fixed64Value:             9223372036854775807,
		Fixed32Value:             4294967295,
		BoolValue:                true,
		StringValue:              "strprefix/foo",
		Uint32Value:              4294967295,
		Sfixed32Value:            2147483647,
		Sfixed64Value:            -4611686018427387904,
		Sint32Value:              2147483647,
		Sint64Value:              4611686018427387903,
		NonConventionalNameValue: "camelCase",
	}

	for i := 0; i < b.N; i++ {
		c.Publish(context.Background(), "test_topic", &ps, true)
	}

	b.ResetTimer()
	ts := TestSubscriber{JSON: true}
	ts.Setup(c)
}

// func TestMiddleware(t *testing.T) {
// 	const stringKey = "test"
// 	foundValue := false
// 	middlewareCalled := false

// 	mw := func(opts pubsub.HandlerOptions, next pubsub.MsgHandler) pubsub.MsgHandler {
// 		return func(ctx context.Context, m pubsub.Msg) error {
// 			ctx = context.WithValue(ctx, stringKey, "testvalue")
// 			err := next(ctx, m)
// 			middlewareCalled = true
// 			return err
// 		}
// 	}

// 	mw2 := func(opts pubsub.HandlerOptions, next pubsub.MsgHandler) pubsub.MsgHandler {
// 		return func(ctx context.Context, m pubsub.Msg) error {
// 			if v := ctx.Value("test"); v != nil {
// 				foundValue = true
// 			}

// 			return next(ctx, m)
// 		}
// 	}

// 	m := &memory.MemoryProvider{}
// 	c := &pubsub.Client{
// 		ServiceName:          "test",
// 		Provider:             m,
// 		SubscriberMiddleware: []pubsub.SubscriberMiddleware{mw, mw2},
// 	}

// 	ps := gw.ABitOfEverything{
// 		StringValue: "strprefix/foo",
// 	}

// 	err := c.Publish(context.Background(), "test_topic", &ps, false)
// 	assert.Nil(t, err)

// 	ts := TestSubscriber{}
// 	ts.Setup(c)

// 	assert.True(t, foundValue)
// 	assert.True(t, middlewareCalled)
// }
