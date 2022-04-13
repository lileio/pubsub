package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/lileio/pubsub/v2"
)

type MemoryProvider struct {
	mutex        sync.RWMutex
	Msgs         map[string][]*pubsub.Msg
	ErrorHandler func(err error)
}

func (mp *MemoryProvider) Publish(ctx context.Context, topic string, m *pubsub.Msg) error {
	mp.mutex.Lock()
	defer mp.mutex.Unlock()

	if mp.Msgs == nil {
		mp.Msgs = make(map[string][]*pubsub.Msg, 0)
	}

	mp.Msgs[topic] = append(mp.Msgs[topic], m)

	return nil
}

func (mp *MemoryProvider) Subscribe(opts pubsub.HandlerOptions, h pubsub.MsgHandler) {
	mp.mutex.RLock()
	defer mp.mutex.RUnlock()

	for _, v := range mp.Msgs[opts.Topic] {
		err := h(context.Background(), *v)

		if err != nil {
			if mp.ErrorHandler != nil {
				mp.ErrorHandler(err)
			} else {
				fmt.Print(err.Error())
			}
		}
	}
	return
}

func (mp *MemoryProvider) Shutdown() {
	return
}
