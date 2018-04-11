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

	err = c.Provider.Publish(ctx, topic, Msg{Data: b})
	if err != nil {
		return err
	}

	return nil
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
