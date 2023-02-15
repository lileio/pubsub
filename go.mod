module github.com/lileio/pubsub/v2

go 1.13

require (
	cloud.google.com/go v0.95.0
	cloud.google.com/go/pubsub v1.17.0
	github.com/dropbox/godropbox v0.0.0-20180512210157-31879d3884b9
	github.com/gofrs/uuid v3.1.0+incompatible
	github.com/golang/protobuf v1.5.2
	github.com/jpillora/backoff v1.0.0
	github.com/lileio/logr v1.1.0
	github.com/nats-io/nats-server/v2 v2.6.1 // indirect
	github.com/nats-io/nats-streaming-server v0.22.1 // indirect
	github.com/nats-io/stan.go v0.10.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/openzipkin-contrib/zipkin-go-opentracing v0.4.3
	github.com/pierrec/lz4 v2.0.5+incompatible // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.1
	github.com/segmentio/kafka-go v0.1.0
	github.com/segmentio/ksuid v1.0.2
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/otel v1.0.1
	go.opentelemetry.io/otel/trace v1.0.1
	go.uber.org/atomic v1.4.0 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.9.1
	golang.org/x/oauth2 v0.0.0-20210819190943-2bc19b11175f
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/api v0.57.0
	google.golang.org/genproto v0.0.0-20210921142501-181ce0d877f6
	google.golang.org/grpc v1.40.0
)
