package memory

import (
	"context"

	"github.com/lileio/pubsub"
	"github.com/siddontang/go/log"
)

type MemoryProvider struct {
	Msgs map[string][]pubsub.Msg
}

func (mp *MemoryProvider) Publish(ctx context.Context, topic string, m pubsub.Msg) error {
	if mp.Msgs == nil {
		mp.Msgs = make(map[string][]pubsub.Msg, 0)
	}

	mp.Msgs[topic] = append(mp.Msgs[topic], m)

	return nil
}

func (mp *MemoryProvider) Subscribe(opts pubsub.HandlerOptions, h pubsub.MsgHandler) {
	for _, v := range mp.Msgs[opts.Topic] {
		err := h(context.Background(), v)

		if err != nil {
			log.Error(err)
		}
	}
	return
}
