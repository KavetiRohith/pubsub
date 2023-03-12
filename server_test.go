package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"nhooyr.io/websocket"
)

func Test_chatServer(t *testing.T) {
	t.Parallel()

	// This is a simple echo test with a single client.
	// The client sends a message and ensures it receives
	// it on its WebSocket.
	t.Run("simple", func(t *testing.T) {
		t.Parallel()

		url, closeFn := setupTest(t)
		defer closeFn()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		cl, err := newClient(ctx, url)
		assertSuccess(t, err)
		defer cl.Close()

		expMsg := randPublishMsg()

		err = cl.sendSubMessage(ctx, expMsg.Topic)
		assertSuccess(t, err)

		err = cl.publish(ctx, expMsg)
		assertSuccess(t, err)

		msg, err := cl.nextMessage(ctx)
		assertSuccess(t, err)

		if expMsg.Topic != msg.Topic || string(*(expMsg.Message)) != string(*(msg.Message)) {
			t.Fatalf("expected %s %s but got %s %s", expMsg.Topic, string(*(expMsg.Message)), msg.Topic, string(*(msg.Message)))
		}
	})

	// This test is a complex concurrency test.
	// 10 clients are started that send 128 different
	// messages concurrently.
	//
	// The test verifies that every message is seen by every client
	// and no errors occur anywhere.
	t.Run("concurrency", func(t *testing.T) {
		t.Parallel()

		const nmessages = 128
		const nclients = 10

		url, closeFn := setupTest(t)
		defer closeFn()

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		var clients []*client
		var clientMsgs []map[PublishMessage]struct{}
		for i := 0; i < nclients; i++ {
			cl, err := newClient(ctx, url)
			assertSuccess(t, err)
			defer cl.Close()

			clients = append(clients, cl)
			clientMsgs = append(clientMsgs, randMessages(nmessages))
		}

		allMessages := make(map[PublishMessage]struct{})
		for _, msgs := range clientMsgs {
			for m := range msgs {
				allMessages[m] = struct{}{}
			}
		}

		for _, cl := range clients {
			for m := range allMessages {
				err := cl.sendSubMessage(ctx, m.Topic)
				assertSuccess(t, err)
			}
		}

		var wg sync.WaitGroup
		for i, cl := range clients {
			i := i
			cl := cl

			wg.Add(1)
			go func() {
				defer wg.Done()
				err := cl.publishMsgs(ctx, clientMsgs[i])
				if err != nil {
					t.Errorf("client %d failed to publish all messages: %v", i, err)
				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				err := testAllMessagesReceived(cl, nclients*nmessages, allMessages)
				if err != nil {
					t.Errorf("client %d failed to receive all messages: %v", i, err)
				}
			}()
		}

		wg.Wait()
	})
}

// setupTest sets up chatServer that can be used
// via the returned url.
//
// Defer closeFn to ensure everything is cleaned up at
// the end of the test.
//
// chatServer logs will be logged via t.Logf.
func setupTest(t *testing.T) (url string, closeFn func()) {
	cs := NewServerWithOpts(
		4096,
		t.Logf,
		rate.NewLimiter(rate.Inf, math.MaxInt),
		func(a, b string) bool { return a == b },
	)

	// To ensure tests run quickly under even -race.
	cs.subscriberMessageBufferLength = 4096
	cs.publishLimiter.SetLimit(rate.Inf)

	s := httptest.NewServer(cs)
	return s.URL, func() {
		s.Close()
	}
}

// testAllMessagesReceived ensures that after n reads, all msgs in msgs
// have been read.
func testAllMessagesReceived(cl *client, n int, msgs map[PublishMessage]struct{}) error {
	msgs = cloneMessages(msgs)

	for i := 0; i < n; i++ {
		recvd, err := cl.nextMessage(context.Background())
		if err != nil {
			return err
		}
		for x := range msgs {
			if x.Topic == recvd.Topic || string(*(x.Message)) == string(*(recvd.Message)) {
				delete(msgs, x)
			}
		}
	}

	if len(msgs) != 0 {
		return fmt.Errorf("did not receive all expected messages: %v", msgs)
	}
	return nil
}

func cloneMessages(msgs map[PublishMessage]struct{}) map[PublishMessage]struct{} {
	msgs2 := make(map[PublishMessage]struct{}, len(msgs))
	for m := range msgs {
		msgs2[m] = struct{}{}
	}
	return msgs2
}

func randMessages(n int) map[PublishMessage]struct{} {
	msgs := make(map[PublishMessage]struct{})
	for i := 0; i < n; i++ {
		m := randPublishMsg()
		if _, ok := msgs[m]; ok {
			i--
			continue
		}
		msgs[m] = struct{}{}
	}
	return msgs
}

func assertSuccess(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

type client struct {
	url string
	c   *websocket.Conn
}

func newClient(ctx context.Context, url string) (*client, error) {
	c, _, err := websocket.Dial(ctx, url+"/subscribe", nil)
	if err != nil {
		return nil, err
	}

	cl := &client{
		url: url,
		c:   c,
	}

	return cl, nil
}

func (cl *client) publish(ctx context.Context, msg PublishMessage) (err error) {
	defer func() {
		if err != nil {
			cl.c.Close(websocket.StatusInternalError, "publish failed")
		}
	}()

	pubmsg, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, cl.url+"/publish", strings.NewReader(string(pubmsg)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("publish request failed: %v", resp.StatusCode)
	}
	return nil
}

func (cl *client) publishMsgs(ctx context.Context, msgs map[PublishMessage]struct{}) error {
	for m := range msgs {
		err := cl.publish(ctx, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cl *client) sendSubMessage(ctx context.Context, topic string) error {
	data, err := json.Marshal(WsMessage{Action: "subscribe", Topic: topic})
	if err != nil {
		return err
	}
	err = cl.c.Write(ctx, websocket.MessageText, data)
	if err != nil {
		return err
	}
	return nil
}

func (cl *client) nextMessage(ctx context.Context) (PublishMessage, error) {
	var p PublishMessage
	typ, r, err := cl.c.Reader(ctx)
	if err != nil {
		return p, err
	}

	if typ != websocket.MessageText {
		cl.c.Close(websocket.StatusUnsupportedData, "expected text message")
		return p, fmt.Errorf("expected text message but got %v", typ)
	}

	b, err := io.ReadAll(r)
	if err != nil {
		return p, err
	}
	err = json.Unmarshal(b, &p)
	if err != nil {
		return p, err
	}

	return p, nil
}

func (cl *client) Close() error {
	return cl.c.Close(websocket.StatusNormalClosure, "")
}

func randPublishMsg() PublishMessage {
	rand.Seed(time.Now().UnixNano())
	s := fmt.Sprintf("{\"%s\":\"%s\"}", randString(10), randString(10))
	msg := json.RawMessage([]byte(s))
	return PublishMessage{Topic: randString(10), Message: &msg}
}

// randString generates a random string with length n.
func randString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
