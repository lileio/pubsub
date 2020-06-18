package rabbitmq

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	
	"github.com/lileio/pubsub/v2"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

const RoutingKey = "RoutingKey"

type PublishExchange struct{}

var (
	defaultConfig = ProviderConfig{
		AppId:                 "",
		EagerPublishExchanges: nil,
		MaxRedialCount:        5,
		RoutingKeySeparator:   "@",
	}
)

type ProviderConfig struct {
	AmqpUrl               string
	AppId                 string
	EagerPublishExchanges []string
	MaxRedialCount        int
	RoutingKeySeparator   string
}

type Provider struct {
	config ProviderConfig
	conn   *amqp.Connection
	ch     *amqp.Channel

	// To synchronize shutdown
	quit *Event

	// exchange channel map
	exchangeQueuesMux sync.RWMutex
	exchangeQueues    map[string]chan *pubsub.Msg
}

func mergeConfig(config ProviderConfig) ProviderConfig {
	cfg := defaultConfig
	if config.AppId != "" {
		cfg.AppId = config.AppId
	}
	if config.AmqpUrl != "" {
		cfg.AmqpUrl = config.AmqpUrl
	}
	if config.MaxRedialCount != cfg.MaxRedialCount && config.MaxRedialCount != 0 {
		cfg.MaxRedialCount = config.MaxRedialCount
	}
	return cfg
}

// Creates a new Subscriber and sets the AMQP url for the internal rabbitMQ client
//
// You can send a
func NewProvider(config ProviderConfig, eagerExchanges ...string) *Provider {
	providerCfg := mergeConfig(config)
	providerCfg.EagerPublishExchanges = eagerExchanges

	p := &Provider{
		config:            providerCfg,
		exchangeQueuesMux: sync.RWMutex{},
		exchangeQueues:    make(map[string]chan *pubsub.Msg),
		quit:              NewEvent(),
	}

	// setup eager exchanges if any
	go p.setupPublishTopics()

	return p
}

// Publish queues a message to a topic (a.k.a an exchange)
//
// In order to supply a routing key for a topic use the following syntax when supplying the topic parameter:
//  // the default config.RoutingKeySeparator is '@'
//  topic := "routingKeys@TopicExchange"
//
// You can also use the helper method provider.RoutingKeyAtExchangeHelper, which uses the separator from the config
func (p *Provider) Publish(ctx context.Context, topic string, message *pubsub.Msg) error {
	sp, _ := opentracing.StartSpanFromContext(ctx, currentFunc())
	defer sp.Finish()

	exchange, routing, err := p.extractExchangeAndRouting(topic)
	if err != nil {
		return err
	}
	queue, err := p.exchangeQueue(exchange)
	if err != nil {
		return err
	}
	message.Metadata[RoutingKey] = routing

	// post message
	queue <- message
	return nil
}

// Subscribes to a specific exchange by creating a new queue consumer
//
// By default the routing key is derived from the Name field of the opts parameter.
// In order to set an empty routing key, use "-" for the mandatory pubsub.HandlerOptions.Name parameter
func (p *Provider) Subscribe(opts pubsub.HandlerOptions, handler pubsub.MsgHandler) {
	go func() {
		queue := identity()

		for session := range p.redial() {
			sub := <-session

			if _, err := sub.QueueDeclare(queue, false, true, false, false, nil); err != nil {
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

			deliveries, err := sub.Consume(queue, "", opts.AutoAck, false, false, false, nil)
			if err != nil {
				logrus.Warnf("cannot consume from: %q, %v", queue, err)
				return
			}

			logrus.Infof("Queue (%s) attached to exchange '%s/%s' via routing key '%s'", queue, sub.Connection.Config.Vhost, exchange, routingKey)

			for msg := range deliveries {
				if p.quit.HasFired() {
					logrus.Infof("Shutting down queue (%s) attached to exchange '%s/%s' via routing key '%s'", queue, sub.Connection.Config.Vhost, exchange, routingKey)
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
				if err != nil && opts.AutoAck {
					returnMessage.Nack()
					continue
				}

				if opts.AutoAck {
					returnMessage.Ack()
				}
			}
		}
	}()
}

// Shutdown is to be called for graceful termination
func (p *Provider) Shutdown() {
	// trigger shutdown
	p.quit.Fire()

	// TODO should wait for all go-rotuines to shut down

	if p.ch != nil {
		_ = p.ch.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
}

// Helpers

// RoutingKeyAtExchangeHelper is a shorthand for creating a publish topic string
//
// It is essentially the same as calling:
//  topic := fmt.Sprintf("%s%s%s", routingKey, p.config.RoutingKeySeparator, exchange)
func (p *Provider) RoutingKeyAtExchangeHelper(routingKey, exchange string) (topic string) {
	return fmt.Sprintf("%s%s%s", routingKey, p.config.RoutingKeySeparator, exchange)
}

// Prepares and creates queues for eager publish exchanges and starts processing on them
func (p *Provider) setupPublishTopics() {
	for _, exchange := range p.config.EagerPublishExchanges {
		exchange, _, err := p.extractExchangeAndRouting(exchange)
		if err != nil {
			logrus.Warnf("Failed to eagerly prepare publish exchange: %s. %v", exchange, err)
			continue
		}
		_, err = p.exchangeQueue(exchange)
		if err != nil {
			logrus.Warnf("Failed to eagerly create queue for publish exchange: %s. %v", exchange, err)
		}
	}
}

// Returns the underlying queue for this topic exchange
//
// If the exchange has not been used before, start a new redial go-routine for processing the queue
func (p *Provider) exchangeQueue(exchange string) (chan *pubsub.Msg, error) {
	p.exchangeQueuesMux.RLock()
	if xch, ok := p.exchangeQueues[exchange]; !ok {
		p.exchangeQueuesMux.RUnlock()
		// create new queue
		newQueue := make(chan *pubsub.Msg, 10)

		// save for reuse
		p.exchangeQueuesMux.Lock()
		p.exchangeQueues[exchange] = newQueue
		p.exchangeQueuesMux.Unlock()

		// start processing until context exists
		go p.publishToExchange(p.redial(), exchange, newQueue)

		return newQueue, nil
	} else {
		p.exchangeQueuesMux.RUnlock()
		return xch, nil
	}
}

// Separates the incoming publish topic to exchange name and routing key
func (p *Provider) extractExchangeAndRouting(ogTopic string) (exchange, routingKey string, err error) {
	if !strings.Contains(ogTopic, p.config.RoutingKeySeparator) {
		return ogTopic, "", nil // is exchange
	}
	s := strings.Split(ogTopic, p.config.RoutingKeySeparator)
	if len(s) < 2 {
		return "", "", errors.New(fmt.Sprintf("Too many separators '%s'. Syntax is: '$routingKey%s$exchange'", p.config.RoutingKeySeparator, p.config.RoutingKeySeparator))
	}
	return s[1], s[0], nil
}

// publishToTopic keeps a reconnecting session to a topic exchange open and waits for queued messages.
//
// It receives from the application specific source of messages.
func (p *Provider) publishToExchange(sessions chan chan session, exchange string, messages <-chan *pubsub.Msg) {
	for session := range sessions {
		var (
			running bool
			reading = messages
			pending = make(chan *pubsub.Msg, 1)
			confirm = make(chan amqp.Confirmation, 1)
		)

		pub := <-session

		// publisher confirms for this channel/connection
		if err := pub.Confirm(false); err != nil {
			logrus.Warnf("publisher confirms not supported")
			close(confirm) // confirms not supported, simulate by always nacking
		} else {
			pub.NotifyPublish(confirm)
		}

	Publish:
		for {
			if p.quit.HasFired() {
				logrus.Infof("Shutting down publishToExchange: %s/%s...", p.conn.Config.Vhost, exchange)
				if pending != nil {
					close(pending)
				}
				if confirm != nil {
					close(confirm)
				}
				return
			}
			var msg *pubsub.Msg
			select {
			// confirm
			case confirmed, ok := <-confirm:
				if !ok {
					break Publish
				}
				if !confirmed.Ack {
					logrus.Debugf("nack message %d, msg: %q", confirmed.DeliveryTag, string(msg.Data))
				}
				reading = messages
			// pending
			case msg = <-pending:
				routingKey := "" // "ignored for fanout exchanges, application dependent for other exchanges"
				if rk, ok := msg.Metadata[RoutingKey]; ok {
					routingKey = rk
				}

				// transfer metadata
				headers := amqp.Table{}
				for k, v := range msg.Metadata {
					headers[k] = v
				}
				publishing := amqp.Publishing{
					Headers: headers,
					// ContentType:     "",
					// ContentEncoding: "",
					// DeliveryMode:    0,
					// Priority:        0,
					// CorrelationId:   "",
					// ReplyTo:         "",
					// Expiration:      "",
					// Type:            "",
					// UserId:          "",
					AppId: p.config.AppId,
					Body:  msg.Data,
				}
				if msg.ID != "" {
					publishing.MessageId = msg.ID
				}
				if msg.PublishTime != nil {
					publishing.Timestamp = *msg.PublishTime
				}
				err := pub.Publish(exchange, routingKey, false, false, publishing)
				// Retry failed delivery on the next session
				if err != nil {
					logrus.Errorf("failed to publish %q. %v", string(msg.Data), err)
					pending <- msg
					_ = pub.Channel.Close()
					break Publish
				} else {
					// write back publish time
					if msg.PublishTime == nil {
						now := time.Now()
						msg.PublishTime = &now
					}
					logrus.Debugf("sent %q", string(msg.Data))
				}
			// reading
			case msg, running = <-reading:
				// all messages consumed
				if !running {
					return
				}
				// work on pending delivery until ack'd
				pending <- msg
				reading = nil
			// loop over for shudown check
			case <-time.After(time.Millisecond * 100):
			}
		}
	}
}

// redial continually connects to the URL
//
// This method exits the program when retry count reaches max retry count defined in the config
func (p *Provider) redial() chan chan session {
	sessions := make(chan chan session)

	go func() {
		sess := make(chan session)
		defer func() {
			close(sessions)
			close(sess)
		}()
		retryCounter := 1
		for {
			select {
			case sessions <- sess:
			case <-p.quit.Done():
				logrus.Info("Shutting down redial:sessionfactory...")
				return
			}

			if p.conn == nil {
				// initialize connection only once
				var err error
				p.conn, err = amqp.Dial(p.config.AmqpUrl)
				if err != nil {
					if retryCounter > p.config.MaxRedialCount {
						logrus.Fatalf("cannot (re)dial: %v: %q", err, p.config.AmqpUrl)
					} else {
						sleep := time.Second * time.Duration(retryCounter)
						logrus.Errorf("waiting %s before redialing AQMP address. Error: %v")
						// sleep before next retry
						time.Sleep(sleep)
						retryCounter++
						continue // try again
					}
				} else {
					retryCounter = 1 // reset retry counter
				}
			}

			ch, err := p.conn.Channel()
			if err != nil {
				logrus.Fatalf("cannot create channel: %v", err)
			}

			// declare short-lived exchanges
			//if err := ch.ExchangeDeclare(exchange, "fanout", false, true, false, false, nil); err != nil {
			//	logrus.Fatalf("cannot declare fanout exchange: %v", err)
			//}

			select {
			case sess <- session{p.conn, ch}:
			case <-p.quit.Done():
				logrus.Info("Shutting down redial:sessions...")
				return
			}
		}
	}()

	return sessions
}
