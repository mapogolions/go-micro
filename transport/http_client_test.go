package transport

import (
	"fmt"
	"sync"
	"testing"

	"github.com/pkg/errors"
)

func TestHttpTransportClient(t *testing.T) {
	t.Run("send recv race condition", func(t *testing.T) {
		testCase := func() {
			// arrange
			l, c, err := echoHttpClient("127.0.0.1:")
			if err != nil {
				t.Error(err)
			}
			defer l.Close()
			defer c.Close()

			// act + assert
			N := cap(c.req)
			if err := send(c, N+1); err != nil {
				t.Errorf("Unexpected send err: %v", err)
			}
			// consume all requests from the buffered channel
			if err := recv(c, N); err != nil {
				t.Errorf("Unexpected recv err: %v", err)
			}
			if len(c.reqList) != 1 {
				t.Error("Unexpected reqList")
			}

			wg := sync.WaitGroup{}
			wg.Add(2)
			msg := Message{}

			go func() {
				defer wg.Done()
				if err := c.Recv(&msg); err != nil {
					t.Errorf("Unexpected recv err: %v", err)
				}
				got := msg.Header[CorrelationID]
				want := fmt.Sprintf("%d", N)
				if want != got { // check race condition. Sometimes got 101 instead of 100
					t.Errorf("Unexpected correlationID: got %q, want %q", got, want)
				}
			}()
			go func() {
				defer wg.Done()
				if err := c.Send(&Message{Body: []byte(fmt.Sprintf("msg-%d", N+1))}); err != nil {
					t.Errorf("Unexpected send err: %v", err)
				}
			}()

			wg.Wait()
		}

		// Increase the number of iterations to raise the likelihood of reproducing the race condition.
		for i := 0; i < 500; i++ {
			testCase()
		}
	})

	t.Run("blocking recv", func(t *testing.T) {
		// arrange
		l, c, err := echoHttpClient("127.0.0.1:")
		if err != nil {
			t.Error(err)
		}
		defer l.Close()
		defer c.Close()

		// act + assert
		N := cap(c.req)
		if err := send(c, N+1); err != nil { // send N+1 message to overflow buffered chan
			t.Errorf("Unexpected send err: %v", err)
		}
		if err := recv(c, N); err != nil { // consume all requests from buffred chan
			t.Errorf("Unexpected recv err: %v", err)
		}
		if len(c.reqList) != 1 {
			t.Error("Unexpected reqList")
		}

		msg := Message{}
		if err := c.Recv(&msg); err != nil {
			t.Errorf("Unexpected recv err: %v", err)
		}
		want := fmt.Sprintf("msg-%d", N)
		got := string(msg.Body)
		if want != got {
			t.Errorf("Unexpected message: got %q, want %q", got, want)
		}
	})
}

func send(c *httpTransportClient, n int) error {
	for i := 0; i < n; i++ {
		body := fmt.Sprintf("msg-%d", i)
		if err := c.Send(&Message{Body: []byte(body)}); err != nil {
			return err
		}
	}
	return nil
}

func recv(c *httpTransportClient, n int) error {
	for i := 0; i < n; i++ {
		msg := Message{}
		if err := c.Recv(&msg); err != nil {
			return err
		}
	}
	return nil
}

func echoHttpClient(addr string) (*httpTransportListener, *httpTransportClient, error) {
	tr := NewHTTPTransport()
	l, err := tr.Listen(addr)
	if err != nil {
		return nil, nil, errors.Errorf("Unexpected listen err: %v", err)
	}
	c, err := tr.Dial(l.Addr())
	if err != nil {
		return nil, nil, errors.Errorf("Unexpected dial err: %v", err)
	}
	go l.Accept(echoHandler)
	return l.(*httpTransportListener), c.(*httpTransportClient), nil
}

func echoHandler(sock Socket) {
	defer sock.Close()
	for {
		var msg Message
		if err := sock.Recv(&msg); err != nil {
			return
		}
		if err := sock.Send(&msg); err != nil {
			return
		}
	}
}
