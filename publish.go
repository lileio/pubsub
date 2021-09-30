package pubsub

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/golang/protobuf/proto"
	"golang.org/x/sync/errgroup"
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
	return mw(c.ServiceName, func(ctx context.Context, topic string, m *Msg) error {
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
		var eg errgroup.Group

		for _, c := range clients {
			cl := c
			publishWaitGroup.Add(1)
			eg.Go(func() error {
				defer publishWaitGroup.Done()
				return cl.Publish(ctx, topic, msg, false)
			})
		}

		err := eg.Wait()
		if err != nil {
			pr.Err = err
		}

		close(pr.Ready)
	}()

	return pr
}

// PublishJSON is a convenience message which publishes to the
// current (global) publisher as JSON
func PublishJSON(ctx context.Context, topic string, obj interface{}) *PublishResult {
	pr := &PublishResult{Ready: make(chan struct{})}
	var wg sync.WaitGroup

	for _, c := range clients {
		publishWaitGroup.Add(1)
		wg.Add(1)
		go func(c *Client) {
			defer publishWaitGroup.Done()
			defer wg.Done()
			err := c.Publish(ctx, topic, obj, true)
			if err != nil {
				pr.Err = err
			}
		}(c)
	}

	wg.Wait()
	close(pr.Ready)
	return pr
}

// WaitForAllPublishing waits for all in flight publisher messages to go, before returning
func WaitForAllPublishing() {
	publishWaitGroup.Wait()
}

func chainPublisherMiddleware(mw ...Middleware) func(serviceName string, next PublishHandler) PublishHandler {
	return func(serviceName string, final PublishHandler) PublishHandler {
		return func(ctx context.Context, topic string, m *Msg) error {
			last := final
			for i := len(mw) - 1; i >= 0; i-- {
				last = mw[i].PublisherMsgInterceptor(serviceName, last)
			}
			return last(ctx, topic, m)
		}
	}
}
