package pubsub_test

import (
	"context"
	"testing"

	gw "github.com/grpc-ecosystem/grpc-gateway/examples/examplepb"
	"github.com/lileio/pubsub"
	"github.com/lileio/pubsub/memory"
)

type TestSubscriber struct{}

func (ts *TestSubscriber) DoSomething(ctx context.Context, t *gw.ABitOfEverything, msg *pubsub.Msg) error {
	return nil
}

func (ts *TestSubscriber) Setup(c *pubsub.Client) {
	c.On(pubsub.HandlerOptions{Topic: "test_topic", Name: "do_something", Handler: ts.DoSomething})
}

func TestSubscribers(t *testing.T) {
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

	for i := 0; i < 10; i++ {
		c.Publish(context.Background(), "test_topic", &ps)
	}

	ts := TestSubscriber{}
	ts.Setup(c)
}

func BenchmarkSubscribers(b *testing.B) {
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
		c.Publish(context.Background(), "test_topic", &ps)
	}

	b.ResetTimer()
	ts := TestSubscriber{}
	ts.Setup(c)
}
