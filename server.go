package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"nhooyr.io/websocket"
)

// Server enables broadcasting to a set of subscribers.
type Server struct {
	// subscriberMessageBufferLength controls the max number
	// of messages that can be queued for a subscriber
	// before it is kicked.
	//
	// Defaults to 16.
	subscriberMessageBufferLength int

	// publishLimiter controls the rate limit applied to the publish endpoint.
	//
	// Defaults to one publish every 100ms with a burst of 8.
	publishLimiter *rate.Limiter

	// log controls where logs are sent.
	// Defaults to log.Printf.
	log func(f string, v ...interface{})

	// serveMux routes the various endpoints to the appropriate handler.
	serveMux http.ServeMux

	// topicMatcher returns whether the topics match or not
	topicMatcher func(string, string) bool

	subscribersMu sync.Mutex
	subscribers   map[*subscriber][]string
}

func NewServerWithOpts(subscriberMessageBufferLength int, logger func(f string, v ...interface{}), publishLimiter *rate.Limiter, topicMatcher func(a, b string) bool) *Server {
	cs := &Server{
		subscriberMessageBufferLength: subscriberMessageBufferLength,
		log:                           logger,
		subscribers:                   make(map[*subscriber][]string),
		publishLimiter:                publishLimiter,
		topicMatcher:                  topicMatcher,
	}
	cs.serveMux.HandleFunc("/subscribe", cs.subscribeHandler)
	cs.serveMux.HandleFunc("/publish", cs.publishHandler)

	return cs
}

// subscriber represents a subscriber.
// Messages are sent on the msgs channel and if the client
// cannot keep up with the messages, closeSlow is called.
type subscriber struct {
	msgs      chan PublishMessage
	closeSlow func()
}

func (cs *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cs.serveMux.ServeHTTP(w, r)
}

// publishHandler reads the request body with a limit of 8192 bytes and then publishes
// the received message.
func (cs *Server) publishHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	body := http.MaxBytesReader(w, r.Body, 8192)

	var req PublishMessage
	dec := json.NewDecoder(body)
	err := dec.Decode(&req)
	if err != nil {
		if errors.Is(err, &http.MaxBytesError{}) {
			http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	cs.publish(PublishMessage{Message: req.Message, Topic: req.Topic})

	w.WriteHeader(http.StatusAccepted)
}

// subscribeHandler accepts the WebSocket connection and then subscribes
// it to all future messages.
func (cs *Server) subscribeHandler(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		cs.log("%v", err)
		return
	}
	defer c.Close(websocket.StatusInternalError, "")

	err = cs.subscribe(r.Context(), c)
	if errors.Is(err, context.Canceled) {
		return
	}
	if websocket.CloseStatus(err) == websocket.StatusNormalClosure ||
		websocket.CloseStatus(err) == websocket.StatusGoingAway {
		return
	}
	if err != nil {
		cs.log("%v", err)
		return
	}
}

// subscribe subscribes the given WebSocket to all broadcast messages.
// It creates a subscriber with a buffered msgs chan to give some room to slower
// connections and then registers the subscriber. It then listens for all messages
// and writes them to the WebSocket. If the context is cancelled or
// an error occurs, it returns and deletes the subscription.
//
// It uses readFromConn to keep reading from the connection to process control
// messages and cancel the context if the connection drops.
func (cs *Server) subscribe(ctx context.Context, c *websocket.Conn) error {

	s := &subscriber{
		msgs: make(chan PublishMessage, cs.subscriberMessageBufferLength),
		closeSlow: func() {
			c.Close(websocket.StatusPolicyViolation, "connection too slow to keep up with messages")
		},
	}
	cs.addSubscriber(s)
	defer cs.deleteSubscriber(s)

	ctx, errChan := cs.readFromConn(ctx, c, s)

	for {
		select {
		case msg := <-s.msgs:
			err := writeTimeout(ctx, time.Second*5, c, msg)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errChan:
			return err
		}
	}
}

// readFromConn to keeps reading from the connection to process control
// subscribe, publish messages and cancel the context if the connection drops.
func (cs *Server) readFromConn(ctx context.Context, c *websocket.Conn, s *subscriber) (context.Context, chan error) {
	ctx, cancel := context.WithCancel(ctx)
	errChan := make(chan error)
	go func() {
		defer cancel()
		for {
			msgType, r, err := c.Reader(ctx)
			if err != nil {
				errChan <- err
				return
			}
			if msgType != websocket.MessageText {
				c.Close(websocket.StatusPolicyViolation, "unexpected data message")
				errChan <- errors.New("unexpected data message")
				return
			}

			v, err := io.ReadAll(r)
			if err != nil {
				errChan <- err
				return
			}

			var msg WsMessage
			err = json.Unmarshal(v, &msg)
			if err != nil {
				errChan <- err
				return
			}

			switch msg.Action {
			case "subscribe":
				cs.addSubscription(s, msg.Topic)
			case "unsubscribe":
				cs.deleteSubscription(s, msg.Topic)
			case "publish":
				cs.publish(PublishMessage{Message: msg.Message, Topic: msg.Topic})
			default:
				c.Close(websocket.StatusPolicyViolation, "unexpected action type")
				errChan <- errors.New("unexpected action type")
			}
		}

	}()
	return ctx, errChan
}

// publish publishes the msg to all subscribers.
// It never blocks and so messages to slow subscribers
// are dropped.
func (cs *Server) publish(msg PublishMessage) {
	cs.subscribersMu.Lock()
	defer cs.subscribersMu.Unlock()

	cs.publishLimiter.Wait(context.Background())

	for s, topics := range cs.subscribers {
		for _, topic := range topics {
			if cs.topicMatcher(topic, msg.Topic) {
				select {
				case s.msgs <- msg:
				default:
					go s.closeSlow()
				}
			}
		}
	}
}

// addSubscriber registers a subscriber.
func (cs *Server) addSubscriber(s *subscriber) {
	cs.subscribersMu.Lock()
	cs.subscribers[s] = []string{}
	cs.subscribersMu.Unlock()
}

// addSubscription adds a subscription
func (cs *Server) addSubscription(s *subscriber, topic string) {
	cs.subscribersMu.Lock()
	cs.subscribers[s] = append(cs.subscribers[s], topic)
	cs.subscribersMu.Unlock()
}

// deleteSubscription deletes a subscription
func (cs *Server) deleteSubscription(s *subscriber, topic string) {
	cs.subscribersMu.Lock()
	defer cs.subscribersMu.Unlock()

	currTopics := cs.subscribers[s]

	for i, currTopic := range currTopics {
		if currTopic == topic {
			cs.subscribers[s] = append(currTopics[:i], currTopics[i+1:]...)
			return
		}
	}

}

// deleteSubscriber deletes the given subscriber.
func (cs *Server) deleteSubscriber(s *subscriber) {
	cs.subscribersMu.Lock()
	delete(cs.subscribers, s)
	cs.subscribersMu.Unlock()
}

func writeTimeout(ctx context.Context, timeout time.Duration, c *websocket.Conn, msg PublishMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return c.Write(ctx, websocket.MessageText, data)
}
