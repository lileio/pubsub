package pubsub

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// Subscribe starts a run loop with a Subscriber that listens to topics and
// waits for a syscall.SIGINT or syscall.SIGTERM
func Subscribe(s Subscriber) {
	s.Setup(client)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
}

// HandlerOptions defines the options for a subscriber handler
type HandlerOptions struct {
	// The topic to subscribe to
	Topic string
	// The name of this subscriber/function
	Name string
	// The name of this subscriber/function's service
	ServiceName string
	// The function to invoke
	Handler Handler
	// A message deadline/timeout
	Deadline time.Duration
	// Concurrency sets the maximum number of msgs to be run concurrently
	// default: 20
	Concurrency int
	// Retries is the amount of times to retry before sending to a dead queue
	// default: 3
	Retries int
	// Auto Ack the message automatically if return err == nil
	AutoAck bool
	// Decode JSON objects from pubsub instead of protobuf
	JSON bool
}

// On takes HandlerOptions and subscribes to a topic, waiting for a protobuf message
// calling the function when a message is received
func (c Client) On(opts HandlerOptions) {
	if opts.Topic == "" {
		panic("lile pubsub: topic must be set")
	}

	if opts.Name == "" {
		panic("lile pubsub: name must be set")
	}

	if opts.Handler == nil {
		panic("lile pubsub: handler cannot be nil")
	}

	// Set some default options
	if opts.Deadline == 0 {
		opts.Deadline = 10 * time.Second
	}

	// Set some default options
	if opts.Retries == 0 {
		opts.Retries = 3
	}

	// Set some default concurrency
	if opts.Concurrency == 0 {
		opts.Concurrency = 20
	}

	// Reflection is slow, but this is done only once on subscriber setup
	hndlr := reflect.TypeOf(opts.Handler)
	if hndlr.Kind() != reflect.Func {
		panic("lile pubsub: handler needs to be a func")
	}

	if hndlr.NumIn() != 3 {
		panic(`lile pubsub: handler should be of format
		func(ctx context.Context, obj *proto.Message, msg *Msg) error
		but didn't receive enough args`)
	}

	if hndlr.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		panic(`lile pubsub: handler should be of format
		func(ctx context.Context, obj *proto.Message, msg *Msg) error
		but first arg was not context.Context`)
	}

	if !opts.JSON {
		if !hndlr.In(1).Implements(reflect.TypeOf((*proto.Message)(nil)).Elem()) {
			panic(`lile pubsub: handler should be of format
		func(ctx context.Context, obj *proto.Message, msg *Msg) error
		but second arg does not implement proto.Message interface`)
		}
	}

	if hndlr.In(2) != reflect.TypeOf(&Msg{}) {
		panic(`lile pubsub: handler should be of format
		func(ctx context.Context, obj *proto.Message, msg *Msg) error
		but third arg was not pubsub.Msg`)
	}

	fn := reflect.ValueOf(opts.Handler)

	cb := func(ctx context.Context, m Msg) error {
		var err error
		obj := reflect.New(hndlr.In(1).Elem()).Interface()
		if opts.JSON {
			err = json.Unmarshal(m.Data, obj)
		} else {
			err = proto.Unmarshal(m.Data, obj.(proto.Message))
		}

		if err != nil {
			return errors.Wrap(err, "lile pubsub: could not unmarshal message")
		}

		rtrn := fn.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(obj),
			reflect.ValueOf(&m),
		})
		if len(rtrn) == 0 {
			return nil
		}

		erri := rtrn[0].Interface()
		if erri != nil {
			err = erri.(error)
		}

		return err
	}

	mw := chainMiddleware(c.SubscriberMiddleware...)

	c.Provider.Subscribe(opts, mw(opts, cb))
}

func chainMiddleware(mw ...SubscriberMiddleware) SubscriberMiddleware {
	return func(opts HandlerOptions, final MsgHandler) MsgHandler {
		return func(ctx context.Context, m Msg) error {
			last := final
			for i := len(mw) - 1; i >= 0; i-- {
				last = mw[i](opts, last)
			}
			return last(ctx, m)
		}
	}
}
