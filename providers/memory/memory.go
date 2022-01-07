package memory

import (
	"context"
	"fmt"
	"github.com/lileio/pubsub/v2"
	"sync"
)

type MemoryProvider struct {
	Msgs            *sync.Map
	Subscribers     *sync.Map
	Errors          chan error
}

func NewMemoryProvider() *MemoryProvider {
	mp := &MemoryProvider{
		&sync.Map{},
		&sync.Map{},
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
	if _, ok := mp.Msgs.Load(topic); !ok {
		mp.Msgs.Store(topic, make(chan *pubsub.Msg, 100))
		mp.Subscribers.Store(topic, make([]pubsub.MsgHandler, 0, 0))
		go mp.process(topic)
	}
}

func (mp *MemoryProvider) Publish(ctx context.Context, topic string, m *pubsub.Msg) error {
	if _, ok := mp.Msgs.Load(topic); !ok {
		mp.SetupTopic(topic)
	}
	c, _ := mp.Msgs.Load(topic)
	c.(chan *pubsub.Msg) <- m
	return nil
}

func (mp *MemoryProvider) Subscribe(opts pubsub.HandlerOptions, h pubsub.MsgHandler) {
	topic := opts.Topic
	if _, ok := mp.Subscribers.Load(topic); !ok {
		mp.SetupTopic(topic)
	}
	s, _ := mp.Subscribers.Load(topic)
	mp.Subscribers.Store(topic, append(s.([]pubsub.MsgHandler), h))
	return
}

func (mp *MemoryProvider) process(topic string) {
	var err error
	c, _ := mp.Msgs.Load(topic)
	for msg := range c.(chan *pubsub.Msg) {
		s, _ := mp.Subscribers.Load(topic)
		for _, handler := range s.([]pubsub.MsgHandler){
			err = handler(context.Background(), *msg)
			if err != nil {
				mp.Errors <- err
			}
		}
	}
}

func (mp *MemoryProvider) Shutdown() {
	
	mp.Msgs.Range(func (k, v interface{}) bool {
		close(v.(chan *pubsub.Msg))
		return true
	})
	close(mp.Errors)
	return
}
