package udp

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/go-quicktest/qt"
)

// Replies to requests in-process. Announces fail with a connection ID mismatch error on every other
// call, and connects fail while failConnects is positive.
type fakeTracker struct {
	cl           *Client
	announces    atomic.Int64
	failConnects atomic.Int64
}

func (me *fakeTracker) Write(b []byte) (int, error) {
	var h RequestHeader
	if err := binary.Read(bytes.NewReader(b), binary.BigEndian, &h); err != nil {
		return 0, err
	}
	var resp bytes.Buffer
	action := h.Action
	var body any
	switch h.Action {
	case ActionConnect:
		if me.failConnects.Add(-1) >= 0 {
			action = ActionError
			body = []byte(ConnectionIdMissmatchNul)
		} else {
			body = ConnectionResponse{ConnectionId: 42}
		}
	case ActionAnnounce:
		if me.announces.Add(1)%2 == 1 {
			action = ActionError
			body = []byte(ConnectionIdMissmatchNul)
		} else {
			body = AnnounceResponseHeader{}
		}
	default:
		action = ActionError
		body = []byte("unexpected")
	}
	if err := Write(&resp, ResponseHeader{Action: action, TransactionId: h.TransactionId}); err != nil {
		return 0, err
	}
	if bs, ok := body.([]byte); ok {
		resp.Write(bs)
	} else if err := Write(&resp, body); err != nil {
		return 0, err
	}
	// Dispatch inline, as the transaction was registered before the request was written.
	if err := me.cl.Dispatcher.Dispatch(resp.Bytes(), nil); err != nil {
		return 0, err
	}
	return len(b), nil
}

func newFakeTrackerClient() (*Client, *fakeTracker) {
	cl := &Client{Dispatcher: new(Dispatcher)}
	ft := &fakeTracker{cl: cl}
	cl.Writer = ft
	return cl, ft
}

// Resetting connIdIssued after an error response must be synchronized with the reads and writes
// done under Client.mu by concurrent requests. Run with -race.
func TestConnIdIssuedResetRace(t *testing.T) {
	cl, _ := newFakeTrackerClient()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				cl.Announce(ctx, AnnounceRequest{}, Options{}, func(net.Addr) bool { return false })
			}
		}()
	}
	wg.Wait()
}

// An error response to the connect request is handled while Client.mu is held by the writer of the
// request that needed the connection, so it must not try to take the lock again.
func TestConnectErrorResponseDoesNotDeadlock(t *testing.T) {
	cl, ft := newFakeTrackerClient()
	ft.failConnects.Store(1)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		cl.Announce(ctx, AnnounceRequest{}, Options{}, func(net.Addr) bool { return false })
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("announce deadlocked after a connect error response")
	}
	qt.Check(t, qt.IsNil(ctx.Err()))
}
