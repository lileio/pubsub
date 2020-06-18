package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lileio/pubsub/v2"
	"github.com/lileio/pubsub/v2/middleware/defaults"
	"github.com/lileio/pubsub/v2/providers/nats"
)

const HelloTopic = "hello.topic"

type HelloMsg struct {
	Greeting string
	Name     string
}

type Subscriber struct{}

func (s *Subscriber) Setup(c *pubsub.Client) {
	c.On(pubsub.HandlerOptions{
		Topic:   HelloTopic,
		Name:    "print-hello",
		Handler: s.printHello,
		AutoAck: true,
		JSON:    true,
	})
}

func (s *Subscriber) printHello(ctx context.Context, msg *HelloMsg, m *pubsub.Msg) error {
	fmt.Printf("Message received %+v\n\n", m)
	fmt.Printf(msg.Greeting + " " + msg.Name + "\n")
	return nil
}

func main() {
	n, err := nats.NewNats("test-cluster")
	if err != nil {
		log.Fatal(err)
	}

	pubsub.SetClient(&pubsub.Client{
		ServiceName: "my-new-service",
		Provider:    n,
		Middleware:  defaults.Middleware,
	})

	go func() {
		pubsub.Subscribe(&Subscriber{})
	}()

	fmt.Println("Subscribing to queues..")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	pubsub.Shutdown()
}
