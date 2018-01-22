package memory

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/lileio/pubsub"
)

type MemoryProvider struct {
	Msgs map[string][][]byte
}

func (mp *MemoryProvider) Publish(ctx context.Context, topic string, msg proto.Message) error {
	if mp.Msgs == nil {
		mp.Msgs = make(map[string][][]byte, 0)
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	mp.Msgs[topic] = append(mp.Msgs[topic], b)

	return nil
}

func (mp *MemoryProvider) Subscribe(topic, subscriberName string, h pubsub.MsgHandler, deadline time.Duration, autoAck bool) {
	for _, v := range mp.Msgs[topic] {
		h(context.Background(), pubsub.Msg{
			Data: v,
		})
	}
	return
}
