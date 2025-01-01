package transport

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/pkg/errors"
)

func TestHttpTransportConcurrentWrite(t *testing.T) {
	// arrange
	tr := NewHTTPTransport()
	l, err := tr.Listen("127.0.0.1:")
	if err != nil {
		t.Errorf("Unexpected listen err: %v", err)
	}
	done := make(chan struct{})
	go l.Accept(func(s Socket) {
		defer s.Close()
		defer close(done)
		for {
			msg := Message{}
			if err := s.Recv(&msg); err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				t.Errorf("Unexpected recv err: %v", err)
				return
			}
			fmt.Println(string(msg.Body))
		}
	})

	c, err := tr.Dial(l.Addr())
	if err != nil {
		t.Errorf("Unexpected dial err: %v", err)
	}
	defer c.Close()

	// act + assert
	workerFn := func(wg *sync.WaitGroup, id, n int) {
		defer wg.Done()
		for i := 0; i < n; i++ {
			msg := &Message{
				Body: []byte(fmt.Sprintf("[worker-%d]: msg-%d", id, i)),
			}
			if err := c.Send(msg); err != nil {
				t.Errorf("Unexpected send err: %v", err)
			}
		}
	}

	wg := sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go workerFn(&wg, i, 10)
	}
	wg.Wait()
	c.Close()
	<-done

}

func TestHttpTransportClient(t *testing.T) {
	// arrange
	l, c, err := echoHttpTransportClient("127.0.0.1:")
	if err != nil {
		t.Error(err)
	}
	defer l.Close()
	defer c.Close()

	// act + assert
	N := cap(c.req)
	// Send N+1 messages to overflow the buffered channel and place the extra message in the internal buffer
	for i := 0; i < N+1; i++ {
		body := fmt.Sprintf("msg-%d", i)
		if err := c.Send(&Message{Body: []byte(body)}); err != nil {
			t.Errorf("Unexpected send err: %v", err)
		}
	}

	// consume all requests from the buffered channel
	for i := 0; i < N; i++ {
		msg := Message{}
		if err := c.Recv(&msg); err != nil {
			t.Errorf("Unexpected recv err: %v", err)
		}
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
}

func echoHttpTransportClient(addr string) (*httpTransportListener, *httpTransportClient, error) {
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
