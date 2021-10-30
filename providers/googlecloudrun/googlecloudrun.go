package googlecloudrun

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/pubsub"
	"github.com/dropbox/godropbox/errors"
	"github.com/lileio/logr"
	ps "github.com/lileio/pubsub/v2"
	"golang.org/x/oauth2/google"

	"github.com/sirupsen/logrus"
)

const httpPath = "/pubsub"
const queryName = "subName"

var (
	mutex = &sync.Mutex{}
)

// GoogleCloudRun provides google cloud pubsub but specifically tailored to Google Cloud Run, in that is works over http and push and not a long running process
type GoogleCloudRun struct {
	client   *pubsub.Client
	topics   map[string]*pubsub.Topic
	subs     map[string]ps.MsgHandler
	shutdown bool
}

type PushRequest struct {
	Message struct {
		Attributes  map[string]string `json:"attributes"`
		Data        []byte            `json:"data"`
		ID          string            `json:"message_id"`
		PublishTime time.Time         `json:"publish_time"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

// NewGoogleCloudRun creates a new GoogleCloudRun instace for a project
func NewGoogleCloudRun(projectID string) (*GoogleCloudRun, error) {
	c, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, err
	}

	return &GoogleCloudRun{
		client: c,
		topics: map[string]*pubsub.Topic{},
		subs:   map[string]ps.MsgHandler{},
	}, nil
}

func (g *GoogleCloudRun) RegisterHandler(m *http.ServeMux) {
	m.HandleFunc(httpPath, func(w http.ResponseWriter, r *http.Request) {
		var m PushRequest
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, fmt.Sprintf("Could not decode body: %v", err), http.StatusBadRequest)
			return
		}

		sub := r.URL.Query().Get(queryName)
		h, ok := g.subs[sub]
		if !ok {
			http.Error(w, fmt.Sprintf("No subscription handler found"), http.StatusInternalServerError)
			return
		}

		msg := ps.Msg{
			ID:          m.Message.ID,
			Metadata:    m.Message.Attributes,
			Data:        m.Message.Data,
			PublishTime: &m.Message.PublishTime,
			Ack: func() {
				// Only auto ack is supported for cloud run
			},
			Nack: func() {
				http.Error(w, fmt.Sprintf("Cannot process this message, NACK"), http.StatusInternalServerError)
				return
			},
		}

		err := h(r.Context(), msg)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error processing message: %v", err), http.StatusBadRequest)
			return
		}

		// All good we can 200
		w.WriteHeader(http.StatusOK)
		return
	})
}

// Publish implements Publish
func (g *GoogleCloudRun) Publish(ctx context.Context, topic string, m *ps.Msg) error {
	t, err := g.getTopic(context.Background(), topic)
	if err != nil {
		return err
	}

	logr.WithCtx(ctx).Debug("Google Pubsub: Publishing")
	// We don't use the given context here, as it may be after the fact and can be cancelled
	// this also usually happens async
	res := t.Publish(context.Background(), &pubsub.Message{
		Data:       m.Data,
		Attributes: m.Metadata,
	})

	_, err = res.Get(context.Background())
	if err != nil {
		logr.WithCtx(ctx).Error(errors.Wrap(err, "publish get failed"))
	} else {
		logr.WithCtx(ctx).Debug("Google Pubsub: Publish confirmed")
	}

	return err
}

// Subscribe implements Subscribe
func (g *GoogleCloudRun) Subscribe(opts ps.HandlerOptions, h ps.MsgHandler) {
	g.subscribe(opts, h, make(chan bool, 1))
}

// Shutdown shuts down all subscribers gracefully
func (g *GoogleCloudRun) Shutdown() {
	// We rely on the http server to do a graceful shutdown
	g.shutdown = true
}

type CloudRunAPIUrlOnly struct {
	Status struct {
		URL string `json:"url"`
	} `json:"status"`
}

func getCloudRunUrl() (string, error) {
	projectNumber, region, err := getProjectAndRegion()
	if err != nil {
		return "", fmt.Errorf("impossible to get the projectNumber and region from the metadata server with error %+v\n", err)
	}

	// Get the service name from the environment variables
	service := os.Getenv("K_SERVICE")
	if service == "" {
		return "", fmt.Errorf("impossible to get the Cloud Run service name from Environment Variable with error %+v\n", err)
	}

	ctx := context.Background()
	// To perform a call the Cloud Run API, the current service, through the service account, needs to be authenticated
	// The Google Auth default client add automatically the authorization header for that.
	client, err := google.DefaultClient(ctx)

	// Build the request to the API
	cloudRunApi := fmt.Sprintf(
		"https://%s-run.googleapis.com/apis/serving.knative.dev/v1/namespaces/%s/services/%s",
		region,
		projectNumber,
		service,
	)
	// Perform the call
	resp, err := client.Get(cloudRunApi)
	if err != nil {
		return "", fmt.Errorf("error when calling the Cloud Run API %s with error %+v\n", cloudRunApi, err)
	}
	defer resp.Body.Close()

	// Read the body of the response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("impossible to read the Cloud Run API call body with error %+v\n", err)
	}

	// Map the JSON body in a minimal struct. We need only the URL, the struct match only this part in the JSON.
	cloudRunResp := &CloudRunAPIUrlOnly{}
	json.Unmarshal(body, cloudRunResp)
	return cloudRunResp.Status.URL, nil
}

func getProjectAndRegion() (projectNumber string, region string, err error) {
	// Get the region from the metadata server. The project number is returned in the response
	resp, err := metadata.Get("/instance/region")
	if err != nil {
		return
	}
	// response pattern is projects/<projectNumber>/regions/<region>
	r := strings.Split(resp, "/")
	projectNumber = r[1]
	region = r[3]
	return
}

func (g *GoogleCloudRun) subscribe(opts ps.HandlerOptions, h ps.MsgHandler, ready chan<- bool) {
	var err error
	subName := opts.ServiceName + "-" + opts.Name + "--" + opts.Topic
	sub := g.client.Subscription(subName)

	t, err := g.getTopic(context.Background(), opts.Topic)
	if err != nil {
		logrus.Panicf("Can't fetch topic: %s", err.Error())
	}

	ok, err := sub.Exists(context.Background())
	if err != nil {
		logrus.Panicf("Can't connect to pubsub: %s", err.Error())
	}

	if !ok {
		serviceURL, err := getCloudRunUrl()
		if err != nil {
			logrus.Panicf("Can't subscribe pubsub topic %s, because: %s", subName, err.Error())
		}

		u, err := url.Parse(serviceURL)
		if err != nil {
			logrus.Panicf("Can't subscribe pubsub topic %s, because service url isn't valid: %s", subName, err.Error())
		}

		if os.Getenv("PUBSUB_SERVICE_ACCOUNT_EMAIL") == "" {
			logrus.Panicf("Can't subscribe pubsub topic %s, PUBSUB_SERVICE_ACCOUNT_EMAIL must be set for invoke cloud run functions", subName)
		}

		// Create a url endpoint like "https://hello-world-askdf6b6k.a.run.app/pubsub?subName=someuniqusubscription"
		u.Path = httpPath
		q := u.Query()
		q.Set(queryName, subName)
		u.RawQuery = q.Encode()

		logrus.Infof("Cloud service URL: %s", serviceURL)

		sc := pubsub.SubscriptionConfig{
			Topic:       t,
			AckDeadline: opts.Deadline,
			PushConfig: pubsub.PushConfig{
				Endpoint: u.String(),
				AuthenticationMethod: &pubsub.OIDCToken{
					ServiceAccountEmail: os.Getenv("PUBSUB_SERVICE_ACCOUNT_EMAIL"),
				},
			},
			DeadLetterPolicy: &pubsub.DeadLetterPolicy{
				MaxDeliveryAttempts: 10,
				DeadLetterTopic:     t.String() + "-dead",
			},
			RetryPolicy: &pubsub.RetryPolicy{
				MinimumBackoff: 30 * time.Second,
				MaximumBackoff: 10 * time.Minute,
			},
		}

		sub, err = g.client.CreateSubscription(context.Background(), subName, sc)
		if err != nil {
			logrus.Panicf("Can't subscribe to topic: %s", err.Error())
		}
	}

	g.subs[subName] = h

	ready <- true
}

func (g *GoogleCloudRun) getTopic(ctx context.Context, name string) (*pubsub.Topic, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if g.topics[name] != nil {
		return g.topics[name], nil
	}

	var err error
	t := g.client.Topic(name)
	ok, err := t.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !ok {
		t, err = g.client.CreateTopic(ctx, name)
		if err != nil {
			return nil, err
		}

		// Also create a dead letter topic
		_, err = g.client.CreateTopic(ctx, name+"-dead")
		if err != nil {
			return nil, err
		}
	}

	t.PublishSettings.DelayThreshold = 200 * time.Millisecond

	g.topics[name] = t

	return t, nil
}

func (g *GoogleCloudRun) deleteTopic(name string) error {
	t, err := g.getTopic(context.Background(), name)
	if err != nil {
		return err
	}

	return t.Delete(context.Background())
}
