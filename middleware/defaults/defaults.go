package defaults

import (
	"github.com/lileio/pubsub"
	"github.com/lileio/pubsub/middleware/logrus"
	"github.com/lileio/pubsub/middleware/opentracing"
	"github.com/lileio/pubsub/middleware/prometheus"
	"github.com/lileio/pubsub/middleware/recover"
)

// Middleware is a helper to import the default middleware for pubsub
var Middleware = []pubsub.Middleware{
	logrus.Middleware{},
	recover.Middleware{},
	opentracing.Middleware{},
	prometheus.Middleware{},
}
