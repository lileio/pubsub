package memory

import (
	"context"
	"encoding/json"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/lileio/pubsub"
)

type MemoryProvider struct {
	Msgs map[string][][]byte
}

func (mp *MemoryProvider) Publish(ctx context.Context, topic string, msg interface{}, isJSON bool) error {
	if mp.Msgs == nil {
		mp.Msgs = make(map[string][][]byte, 0)
	}

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

	mp.Msgs[topic] = append(mp.Msgs[topic], b)

	return nil
}

func (mp *MemoryProvider) Subscribe(topic, subscriberName string, h pubsub.MsgHandler, deadline time.Duration, autoAck bool) {
	for _, v := range mp.Msgs[topic] {
		err := h(context.Background(), pubsub.Msg{
			Data: v,
		})
		if err != nil {
			panic(err)
		}
	}
	return
}
