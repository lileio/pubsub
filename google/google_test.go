package google

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/lileio/lile/test"
	"github.com/lileio/pubsub"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestGooglePublishSubscribe(t *testing.T) {
	key := os.Getenv("GOOGLE_KEY")
	if key != "" {
		err := ioutil.WriteFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"), []byte(key), os.ModePerm)
		if err != nil {
			panic(err)
		}

	}

	if os.Getenv("GCLOUD_PROJECT") == "" {
		t.Skip()
		return
	}

	u1 := uuid.Must(uuid.NewV4())
	sub := "lile_" + u1.String()

	ps, err := NewGoogleCloud(os.Getenv("GCLOUD_PROJECT"))
	assert.Nil(t, err)
	assert.NotNil(t, ps)

	done := make(chan bool)

	topic := "lile_topic"
	_, err = ps.getTopic(topic)
	assert.Nil(t, err)

	opts := pubsub.HandlerOptions{
		Topic:       topic,
		Name:        sub,
		ServiceName: "test",
	}

	a := test.Account{Name: "Alex"}
	ps.subscribe(opts, func(ctx context.Context, m pubsub.Msg) error {
		var ac test.Account
		proto.Unmarshal(m.Data, &ac)

		assert.Equal(t, ac.Name, a.Name)

		done <- true
		return nil
	}, done)

	// Wait for subscription
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		assert.Fail(t, "Subscription failed after timeout")
	}

	assert.Nil(t, ps.deleteTopic(topic))
}
