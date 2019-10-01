![logo](./docs/logo.png)
--
![Actions Status](https://github.com/lileio/pubsub/workflows/Test/badge.svg) [![](https://godoc.org/github.com/lileio/pubsub?status.svg)](http://godoc.org/github.com/lileio/pubsub)

PubSub provides a simple helper library for doing publish and subscribe style asynchronous tasks in Go, usually in a web or micro service. PubSub allows you to write publishers and subscribers, fully typed, and swap out providers (Google Cloud PubSub, AWS SQS etc) as required. 

PubSub also abstracts away the creation of the queues and their subscribers, so you shouldn't have to write any cloud specific code, but still gives you [options](https://godoc.org/github.com/lileio/pubsub#HandlerOptions) to set concurrency, deadlines, error handling etc.

[Middleware](https://github.com/lileio/pubsub/tree/master/middleware) is also included, including logging, tracing and error handling!


## Table of Contents

- [Example](#example)
  * [Publisher](#publisher)
  * [Subscriber](#subscriber)
  * [Full Example](#full-example)
- [Middleware](#middleware)
  * [Default](#default)
  * [Opentracing](#opentracing)
  * [Prometheus](#prometheus)
- [Providers](#providers)
  * [Google PubSub](#google-cloud-pubsub)
  * [Nats Streaming](#nats-streaming-server)
  * [AWS SQS/SNS](#aws-sqssns)
  * [Kakfa](#kafka)

## Example

Here's a basic example using [Nats streaming server](https://nats-io.github.io/docs/nats_streaming/intro.html) and a basic subscriber function that prints hello.


To publish messages, you can call `Publish`, you can publish a [Protobuf](https://developers.google.com/protocol-buffers/docs/gotutorial) or JSON serializable object (i.e, most Go objects).

Publish is Protobuf by default..

### Publisher

``` go
pubsub.Publish(ctx, "topic-name", &User{Id: "usr_0001"})
```

to publish a JSON object

``` go
pubsub.PublishJSON(ctx, "topic-name", &User{Id: "usr_0001"})
```

This can be useful if the application subscribing isn't good with Protobuf or is external to your company for example. However Protobuf is recommended for speed, type safety and forwards compatability.

### Subscriber

Subscribing to a topic is done with a single function, you'll receive a context, the object that was in the queue and the pubsub message, which includes some metadata and timing information, should you need it.

``` go
func PrintHello(ctx context.Context, msg *HelloMsg, m *pubsub.Msg) error {
	fmt.Printf("Message received %+v\n\n", m)

	fmt.Printf(msg.Greeting + " " + msg.Name + "\n")

	return nil
}
```

First though, you need to "Setup" your subscribers

``` go
type Subscriber struct{}

func (s *Subscriber) Setup(c *pubsub.Client) {
	c.On(pubsub.HandlerOptions{
		Topic:   HelloTopic,
		Name:    "print-hello",
		Handler: PrintHello,
		AutoAck: true,
		JSON:    true,
	})
}

pubsub.Subscribe(&Subscriber{})
```

### Full Example

You can see a full example in the [example folder](https://github.com/lileio/pubsub/tree/master/example).

## Middleware

### Default

PubSub provides a helper to setup the [default middleware](https://github.com/lileio/pubsub/blob/master/middleware/defaults/defaults.go).

At the time of writing this includes, [Logrus](https://github.com/sirupsen/logrus), [Opentracing](https://github.com/opentracing/opentracing-go), [Prometheus](https://github.com/lileio/pubsub/blob/master/middleware/prometheus/prometheus.go), [Recovery (Handles panics)](https://github.com/lileio/pubsub/blob/master/middleware/recover/recover.go) and [Audit Logging](https://github.com/lileio/pubsub/blob/master/middleware/audit/audit.go)

To use this, simple include it when initialising PubSub

```go
pubsub.SetClient(&pubsub.Client{
	ServiceName: "my-service-name",
	Provider:    provider,
	Middleware:  defaults.Middleware,
})
```

You can optionaly provide a recovery handler too.

```go
pubsub.SetClient(&pubsub.Client{
	ServiceName: "my-service-name",
	Provider:    provider,
	Middleware:  defaults.MiddlewareWithRecovery(func(p interface{}) (err error){
		// log p or report to an error reporter
	}),
})
```

### Logrus

When enabled, the Logrus middleware will output something similar to below. Note that the level is `DEBUG` by default. To see the logs, you'll need to set `logrus.SetLevel(logrus.DebugLevel)` or use something like [github.com/lileio/Logr](https://github.com/lileio/logr/blob/master/logr.go#L44) which can set it from ENV variables.

```
time="2019-09-23T12:46:13Z" level=debug msg="Google Pubsub: Publishing"
time="2019-09-23T12:46:13Z" level=debug msg="Google Pubsub: Publish confirmed"
time="2019-09-23T12:46:13Z" level=debug msg="Published PubSub Msg" component=pubsub duration=143.545203ms metadata="map[x-b3-parentspanid:622cff2be9102141 x-b3-sampled:1 x-b3-flags:0 x-audit-user:xxxx@example.co.uk x-b3-traceid:4275176f2f7f729257887d1e4853498d x-b3-spanid:017e28147c6c3704]" topic=hello.world
time="2019-09-23T12:46:16Z" level=debug msg="Processed PubSub Msg" component=pubsub duration=1.702259988s handler=function_name id=734207593944188 metadata="map[x-b3-traceid:4275176f2f7f729257887d1e4853498d x-b3-spanid:017e28147c6c3704 x-audit-user:xxxx@example.co.uk x-b3-parentspanid:622cff2be9102141 x-b3-flags:0 x-b3-sampled:1]" topic=hello.work
```

### Opentracing

The Opentracing middle adds tags ands logs to spans which will later be sent to something like [Zipkin](https://zipkin.io/) or [Jaeger](https://www.jaegertracing.io/) when setup in the application. Note that the Opentracing middleware only adds things to the context but isn't responsible for setting up Opentracing and it's reporting, for that, [see here](https://github.com/opentracing/opentracing-go).

### Prometheus

The Prometheus middleware includes some counters and histograms to help with monitoring, you can see there [here](https://github.com/lileio/pubsub/blob/master/middleware/prometheus/prometheus.go#L13) but these include.

```
pubsub_message_published_total{topic,service}
pubsub_outgoing_bytes{topic,service}
pubsub_publish_durations_histogram_seconds
pubsub_server_handled_total{"topic", "service", "success"}
pubsub_incoming_bytes{"topic", "service"}
pubsub_subscribe_durations_histogram_seconds{"topic", "service"}
```

Here's an example query to get messages handled (by a subscriber) every minute, make sure your prometheus step is also `1m`

```
sum(increase(pubsub_server_handled_total[1m])) by (topic, success)
```

## Providers


### Google Cloud PubSub

To setup a Google Cloud client, you can do the following..

``` go
pubsub.SetClient(&pubsub.Client{
	ServiceName: "my-service-name",
	Provider:    google.NewGoogleCloud('projectid'),
	Middleware:  defaults.Middleware,
})
```

If you're on Google Cloud vms you're environment likely already has credentials setup, but locally you can set them up with [default credentials](https://cloud.google.com/sdk/gcloud/reference/auth/application-default/), if you're on Kubernetes, I reccomend setting up service account and then making a secret file and setting the `GOOGLE_APPLICATION_CREDENTIALS` to the filepath of that JSON secret key.

The Google PubSub provider is tested heavily in production by [Echo](http://echo.co.uk) and works well, we have however noticed some strange behaviour from Google subscribers, as they try to be clever and balance traffic and other strange things. For example, if you want to only process 2 messages at a time, and don't process the two you're given, then can often result in a pause before more messages are sent to you, this can be hard to debug as a queue builds up, but often fixes itself.


### Nats Streaming Server

To setup a Nats Streaming client, you can do the following. Optionally passing options for the original [client](github.com/nats-io/stan.go)

``` go
pubsub.SetClient(&pubsub.Client{
	ServiceName: "my-service-name",
	Provider:    nats.NewNats('clustername', opts),
	Middleware:  defaults.Middleware,
})
```

Note this driver is for Nats Streaming, and not for plain Nats.

### AWS SQS/SNS

Currently there is no provider for AWS SNS and SQS. Please feel free to make a pull request!

### Kafka

There's an experimental provider for Kafka available [here](https://github.com/lileio/pubsub/blob/master/providers/kafka/kafka.go), but it's limiting in options you can override. I'd love to see someone take this on and help it become more bullet proof. But things like retries are hard.
