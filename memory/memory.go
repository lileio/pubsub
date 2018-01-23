package memory

import (
	"context"
	"time"

	"github.com/lileio/pubsub"
	"github.com/siddontang/go/log"
)

type MemoryProvider struct {
	Msgs map[string][][]byte
}

func (mp *MemoryProvider) Publish(ctx context.Context, topic string, b []byte) error {
	if mp.Msgs == nil {
		mp.Msgs = make(map[string][][]byte, 0)
	}

	mp.Msgs[topic] = append(mp.Msgs[topic], b)

	return nil
}

func (mp *MemoryProvider) Subscribe(topic, subscriberName string, h pubsub.MsgHandler, deadline time.Duration, autoAck bool) {
	for _, v := range mp.Msgs[topic] {
		err := h(context.Background(), pubsub.Msg{
			Data: v,
		})

		if err != nil {
			log.Error(err)
		}
	}
	return
}
