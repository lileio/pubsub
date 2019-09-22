![logo](https://raw.githubusercontent.com/lileio/pubsub/master/docs/logo.png)
--

PubSub provides a simple helper library for doing publish and subscribe style asynchronous tasks in Go, usually in a web or micro service. PubSub allows you to write publishers and subscribers, fully typed, and swap out providers (Google Cloud PubSub, AWS SQS etc) as required. 

PubSub also abstracts away the creation of the queues and their subscribers, so you shouldn't have to write any cloud specific code, but still gives you [options](https://godoc.org/github.com/lileio/pubsub#HandlerOptions) to set concurrency, deadlines, error handling etc.

[Middleware](https://github.com/lileio/pubsub/tree/master/middleware) is also included, including logging, tracing and error handling!


## Table of Contents

- [Simple Example](#example)
- [FAQ](#faq)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Client Libraries](#client-libraries)
- [Roadmap](#roadmap)
- [Acknowledgements](#acknowledgements)
