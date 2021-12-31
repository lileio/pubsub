package memory

import (
	"context"
	"fmt"
	"github.com/lileio/pubsub/v2"
	"sync"
)

type MemoryProvider struct {
	*sync.Mutex
	Msgs            map[string]chan *pubsub.Msg
	Subscribers     map[string][]pubsub.MsgHandler
	Errors          chan error
}

func NewMemoryProvider() *MemoryProvider {
	mp := &MemoryProvider{
		&sync.Mutex{},
		make(map[string]chan *pubsub.Msg),
		make(map[string][]pubsub.MsgHandler),
		make(chan error, 101),
	}
	go mp.ProcessErrors()
	return mp
}

func (mp *MemoryProvider) ProcessErrors() {
	for err := range mp.Errors {
		fmt.Println(err)
	}
}

func (mp *MemoryProvider) SetupTopic(topic string) {
	mp.Lock()
	defer mp.Unlock()
	if _, ok := mp.Msgs[topic]; !ok {
		mp.Msgs[topic] = make(chan *pubsub.Msg, 100)
		mp.Subscribers[topic] = make([]pubsub.MsgHandler, 0, 0)
		go mp.process(topic)
	}
}

func (mp *MemoryProvider) Publish(ctx context.Context, topic string, m *pubsub.Msg) error {
	if _, ok := mp.Msgs[topic]; !ok {
		mp.SetupTopic(topic)
	}
	mp.Msgs[topic] <- m
	return nil
}

func (mp *MemoryProvider) Subscribe(opts pubsub.HandlerOptions, h pubsub.MsgHandler) {
	mp.Lock()
	defer mp.Unlock()
	topic := opts.Topic
	if _, ok := mp.Subscribers[topic]; !ok {
		mp.SetupTopic(topic)
	}
	mp.Subscribers[topic] = append(mp.Subscribers[topic], h)
	return
}

func (mp *MemoryProvider) process(topic string) {
	var err error
	for msg := range mp.Msgs[topic] {
		for _, handler := range mp.Subscribers[topic] {
			err = handler(context.Background(), *msg)
			if err != nil {
				mp.Errors <- err
			}
		}
	}
}

func (mp *MemoryProvider) Shutdown() {
	mp.Lock()
	defer mp.Unlock()

	for _, c := range mp.Msgs {
		close(c)
	}
	close(mp.Errors)
	return
}
