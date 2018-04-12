package pubsub

import (
	"context"
	"encoding/json"

	"github.com/golang/protobuf/proto"
)

// Publish published on the client
func (c *Client) Publish(ctx context.Context, topic string, msg interface{}, isJSON bool) error {
	var b []byte
	var err error
	if isJSON {
		b, err = json.Marshal(msg)
	} else {
		b, err = proto.Marshal(msg.(proto.Message))
	}

	if err != nil {
		return err
	}

	m := &Msg{Data: b}

	mw := chainPublisherMiddleware(c.Middleware...)
	return mw(func(ctx context.Context, topic string, m *Msg) error {
		return c.Provider.Publish(ctx, topic, m)
	})(ctx, topic, m)
}

// A PublishResult holds the result from a call to Publish.
type PublishResult struct {
	Ready chan struct{}
	Err   error
}

// Publish is a convenience message which publishes to the
// current (global) publisher as protobuf
func Publish(ctx context.Context, topic string, msg proto.Message) *PublishResult {
	pr := &PublishResult{Ready: make(chan struct{})}
	go func() {
		err := client.Publish(ctx, topic, msg, false)
		pr.Err = err
		close(pr.Ready)
	}()
	return pr
}

// PublishJSON is a convenience message which publishes to the
// current (global) publisher as JSON
func PublishJSON(ctx context.Context, topic string, obj interface{}) *PublishResult {
	pr := &PublishResult{Ready: make(chan struct{})}
	go func() {
		err := client.Publish(ctx, topic, obj, true)
		pr.Err = err
		close(pr.Ready)
	}()
	return pr
}

func chainPublisherMiddleware(mw ...Middleware) func(next PublishHandler) PublishHandler {
	return func(final PublishHandler) PublishHandler {
		return func(ctx context.Context, topic string, m *Msg) error {
			last := final
			for i := len(mw) - 1; i >= 0; i-- {
				last = mw[i].PublisherMsgInterceptor(last)
			}
			return last(ctx, topic, m)
		}
	}
}
