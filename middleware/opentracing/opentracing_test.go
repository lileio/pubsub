package opentracing

import (
	"context"
	"testing"

	"github.com/lileio/pubsub"
	"github.com/lileio/pubsub/providers/memory"
	"github.com/lileio/pubsub/test"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	zipkintracer "github.com/openzipkin/zipkin-go-opentracing"
	"github.com/stretchr/testify/assert"
)

type TestSubscriber struct {
	JSON bool
	T    *testing.T
}

func (ts *TestSubscriber) DoSomething(ctx context.Context, t *test.Account, msg *pubsub.Msg) error {
	assert.NotNil(ts.T, opentracing.SpanFromContext(ctx))
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

func TestOpentracingMiddleware(t *testing.T) {
	recorder := zipkintracer.NewInMemoryRecorder()
	tracer, err := zipkintracer.NewTracer(recorder)
	opentracing.SetGlobalTracer(tracer)
	if err != nil {
		t.Fatalf("Unable to create Tracer: %+v", err)
	}

	// A fake span from say, an RPC request
	span := tracer.StartSpan("fake_span")
	span.LogFields(
		log.String("event", "soft error"),
		log.String("sql", "select * from something;"),
		log.Int("waited.millis", 1500))
	span.Finish()

	ctx := opentracing.ContextWithSpan(context.Background(), span)

	m1 := Middleware{Tracer: tracer}

	m := &memory.MemoryProvider{}
	c := &pubsub.Client{
		ServiceName: "test",
		Provider:    m,
		Middleware:  []pubsub.Middleware{m1},
	}

	ps := test.Account{
		Name: "pubsub",
	}

	err = c.Publish(ctx, "test_topic", &ps, false)
	assert.Nil(t, err)

	ts := TestSubscriber{T: t}
	ts.Setup(c)
}
