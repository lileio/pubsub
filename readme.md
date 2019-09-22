![logo](./docs/logo.png)
--

PubSub provides a simple helper library for doing publish and subscribe style asynchronous tasks in Go, usually in a web or micro service. PubSub allows you to write publishers and subscribers, fully typed, and swap out providers (Google Cloud PubSub, AWS SQS etc) as required. 

PubSub also abstracts away the creation of the queues and their subscribers, so you shouldn't have to write any cloud specific code, but still gives you [options](https://godoc.org/github.com/lileio/pubsub#HandlerOptions) to set concurrency, deadlines, error handling etc.

[Middleware](https://github.com/lileio/pubsub/tree/master/middleware) is also included, including logging, tracing and error handling!


## Table of Contents

- [Basic Example](#example)

## Example

Here's a basic example using [Nats streaming server](https://nats-io.github.io/docs/nats_streaming/intro.html) and a basic subscriber function that prints hello.

``` go
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
```

You can test this by first running Nats with the following Docker command

```
docker run -v $(pwd)/data:/datastore -p 4222:4222 -p 8223:8223 nats-streaming -p 4222 -m 8223 -store file -dir /datastore
```

And then publishing a message
