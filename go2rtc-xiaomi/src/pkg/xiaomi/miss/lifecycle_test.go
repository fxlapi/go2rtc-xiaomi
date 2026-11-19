package miss

import (
	"errors"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestMuxConcurrentLastStopAndOpen(t *testing.T) {
	for i := 0; i < 200; i++ {
		key := make([]byte, 32)
		conn := newMuxTestConn(key)
		m := NewMux(&Client{Conn: conn, key: key})
		m.startedMask = 1
		// Drive dispatch directly, as the shared reader would, so only the
		// Open/Stop ownership interleaving is under test.
		m.reading = true
		old := attachTestProducer(t, m, 0, false)
		pumpDone := make(chan struct{})
		var pump sync.WaitGroup
		pump.Add(1)
		go func() {
			defer pump.Done()
			for {
				select {
				case <-pumpDone:
					return
				default:
				}
				m.dispatch(&Packet{CodecID: codecH265, Channel: 0, ChannelOK: true,
					Payload: []byte{0, 0, 0, 1, 0x40, 1, 0x0c, 1}})
				runtime.Gosched()
			}
		}()
		gate := make(chan struct{})
		stopped := make(chan error, 1)
		go func() {
			<-gate
			stopped <- old.Stop()
		}()
		opened := make(chan *Producer, 1)
		failed := make(chan error, 1)
		go func() {
			<-gate
			p, err := m.open("xiaomi://127.0.0.1?channel=0&audio=0")
			if err != nil {
				failed <- err
			} else {
				opened <- p
			}
		}()
		close(gate)
		var next *Producer
		var err error
		select {
		case next = <-opened:
		case err = <-failed:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Open/Stop did not finish")
		}
		stopErr := <-stopped
		close(pumpDone)
		pump.Wait()
		if stopErr != nil {
			t.Fatal(stopErr)
		}
		if next != nil {
			if m.Closed() {
				t.Fatalf("iteration %d: last release closed a newly admitted Producer", i)
			}
			_ = next.Stop()
		} else if !errors.Is(err, io.EOF) || !m.Closed() {
			t.Fatalf("iteration %d: Open returned %v, closed=%v", i, err, m.Closed())
		}
	}
}

type blockedAudioConn struct {
	Conn
	entered chan struct{}
	resume  chan struct{}
	closed  chan struct{}
}

func (c *blockedAudioConn) WritePacket([]byte, []byte) error {
	close(c.entered)
	<-c.resume
	return nil
}

func (c *blockedAudioConn) Close() error {
	close(c.closed)
	return nil
}

func TestMuxCloseWaitsForBackchannelWrite(t *testing.T) {
	conn := &blockedAudioConn{entered: make(chan struct{}), resume: make(chan struct{}), closed: make(chan struct{})}
	m := NewMux(&Client{Conn: conn, key: make([]byte, 32)})
	written := make(chan error, 1)
	go func() { written <- m.writeAudio(codecPCMA, []byte{1, 2, 3}) }()
	<-conn.entered
	closed := make(chan error, 1)
	go func() { closed <- m.Close() }()
	<-m.done // New work is rejected before the underlying client is closed.
	select {
	case <-conn.closed:
		t.Fatal("closed client while its audio write was in flight")
	default:
	}
	close(conn.resume)
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := m.writeAudio(codecPCMA, nil); !errors.Is(err, io.EOF) {
		t.Fatalf("write on closed mux returned %v", err)
	}
	if err := m.startSpeaker(); !errors.Is(err, io.EOF) {
		t.Fatalf("speaker start on closed mux returned %v", err)
	}
}
