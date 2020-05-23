package rabbitmq

import (
	"context"
	"crypto/sha1"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/lileio/pubsub/v2"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// session composes an amqp.Connection with an amqp.Channel
type session struct {
	*amqp.Connection
	*amqp.Channel
}

type RabbitMQProvider struct {
	amqpUrl    string
	conn       *amqp.Connection
	ch         *amqp.Channel
	pubsubDone context.CancelFunc
	shutdown   bool
}

// Creates a new Subscriber and sets the AMQP url for the internal rabbitMQ client
func NewRabbitMQProvider(amqpUrl string) *RabbitMQProvider {
	return &RabbitMQProvider{amqpUrl: amqpUrl}
}

func (r *RabbitMQProvider) Publish(ctx context.Context, topic string, m *pubsub.Msg) error {
	panic("implement me")
}

func (r *RabbitMQProvider) Subscribe(opts pubsub.HandlerOptions, handler pubsub.MsgHandler) {
	go func() {
		queue := identity()

		for session := range r.redial(context.Background(), r.amqpUrl) {
			sub := <-session

			if _, err := sub.QueueDeclare(queue, false, true, true, false, nil); err != nil {
				logrus.Warnf("cannot consume from exclusive queue: %q, %v", queue, err)
				return
			}

			exchange := opts.Topic
			routingKey := ""
			if opts.Name != "-" {
				routingKey = opts.Name
			}

			if err := sub.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
				logrus.Warnf("cannot consume without a binding to exchange: %q, %v", exchange, err)
				return
			}

			deliveries, err := sub.Consume(queue, "", opts.AutoAck, true, false, false, nil)
			if err != nil {
				logrus.Warnf("cannot consume from: %q, %v", queue, err)
				return
			}

			logrus.Infof("Queue (%s) attached to exchange '%s' via routing key '%s'", queue, exchange, routingKey)

			for msg := range deliveries {
				if r.shutdown {
					break
				}

				returnMessage := pubsub.Msg{
					ID: msg.MessageId,
					Metadata: map[string]string{
						"AppId":           msg.AppId,
						"ConsumerTag":     msg.ConsumerTag,
						"ContentEncoding": msg.ContentEncoding,
						"ContentType":     msg.ContentType,
						"CorrelationId":   msg.CorrelationId,
						"Exchange":        msg.Exchange,
						"RoutingKey":      msg.RoutingKey,
						"Expiration":      msg.Expiration,
						"ReplyTo":         msg.ReplyTo,
						"Type":            msg.Type,
						"UserId":          msg.UserId,
						"Priority":        strconv.Itoa(int(msg.Priority)),
						"Redelivered":     strconv.FormatBool(msg.Redelivered),
					},
					Data:        msg.Body,
					PublishTime: &msg.Timestamp,
					Ack: func() {
						err := sub.Ack(msg.DeliveryTag, false)
						warnOnError(err, "Failed to acknowledge message")
					},
					Nack: func() {
						err := sub.Nack(msg.DeliveryTag, false, true) // automatically requeue
						warnOnError(err, "Failed to acknowledge message")
						if err == nil {
							logrus.Debugf("Unprocessed message (%s) successfully requeued", msg.DeliveryTag)
						}
					},
				}

				dlCtx, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.Deadline))

				// handle message
				err := handler(dlCtx, returnMessage)
				if err != nil {
					returnMessage.Nack()
					continue
				}

				if !opts.AutoAck {
					returnMessage.Ack()
				}
			}
		}
	}()
}

// redial continually connects to the URL, exiting the program when no longer possible
func (r *RabbitMQProvider) redial(ctx context.Context, url string) chan chan session {
	sessions := make(chan chan session)

	go func() {
		sess := make(chan session)
		defer close(sessions)

		for {
			select {
			case sessions <- sess:
			case <-ctx.Done():
				logrus.Debugf("shutting down session factory")
				return
			}

			if r.conn == nil {
				// initialize connection only once
				var err error
				r.conn, err = amqp.Dial(url)
				if err != nil {
					logrus.Fatalf("cannot (re)dial: %v: %q", err, url)
				}
			}

			ch, err := r.conn.Channel()
			if err != nil {
				logrus.Fatalf("cannot create channel: %v", err)
			}

			// declare short-lived exchanges
			//if err := ch.ExchangeDeclare(exchange, "fanout", false, true, false, false, nil); err != nil {
			//	logrus.Fatalf("cannot declare fanout exchange: %v", err)
			//}

			select {
			case sess <- session{r.conn, ch}:
			case <-ctx.Done():
				logrus.Debugf("shutting down new session")
				return
			}
		}
	}()

	return sessions
}

func (r *RabbitMQProvider) Shutdown() {
	r.shutdown = true
	if r.pubsubDone != nil {
		r.pubsubDone()
	}
	if r.ch != nil {
		_ = r.ch.Close()
	}
	if r.conn != nil {
		_ = r.conn.Close()
	}
}

// identity returns the same host/process unique string for the lifetime of
// this process so that subscriber reconnections reuse the same queue name.
func identity() string {
	hostname, err := os.Hostname()
	h := sha1.New()
	_, _ = fmt.Fprint(h, hostname)
	_, _ = fmt.Fprint(h, err)
	_, _ = fmt.Fprint(h, os.Getpid())
	return fmt.Sprintf("%x", h.Sum(nil))
}

func warnOnError(err error, msg string) {
	if err != nil {
		logrus.Warnf("%s: %s", msg, err)
	}
}
