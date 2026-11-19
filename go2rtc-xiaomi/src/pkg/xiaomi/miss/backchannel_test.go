package miss

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/xiaomi/miss/cs2"
	"github.com/pion/rtp"
)

type backchannelTestConn struct {
	net.Conn
	mu        sync.Mutex
	sequences []uint16
}

type blockedSpeakerConn struct {
	Conn
	entered chan struct{}
	resume  chan struct{}
}

func (c *blockedSpeakerConn) WriteCommand(uint32, []byte) error {
	close(c.entered)
	<-c.resume
	return nil
}

func (*blockedSpeakerConn) Close() error { return nil }

func TestBackchannelStopDuringSpeakerSetup(t *testing.T) {
	conn := &blockedSpeakerConn{entered: make(chan struct{}), resume: make(chan struct{})}
	m := NewMux(&Client{Conn: conn, key: make([]byte, 32)})
	p := attachTestProducer(t, m, 0, true)
	media := p.Medias[1]
	track := core.NewReceiver(media, media.Codecs[0])
	added := make(chan error, 1)
	go func() { added <- p.AddTrack(media, media.Codecs[0], track) }()
	<-conn.entered
	stopped := make(chan error, 1)
	go func() { stopped <- p.Stop() }()
	<-m.done
	close(conn.resume)
	select {
	case err := <-added:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("AddTrack during stop returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AddTrack did not finish after concurrent Stop")
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if len(p.Senders) != 0 {
		t.Fatal("AddTrack attached a sender after Producer.Stop")
	}
}

func (c *backchannelTestConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.sequences = append(c.sequences, binary.BigEndian.Uint16(p[6:]))
	c.mu.Unlock()
	return len(p), nil
}

// Exercise two backchannel senders sharing the actual CS2 transport. This
// test uses no camera, socket, credentials or network access.
func TestMuxConcurrentBackchannelWrites(t *testing.T) {
	conn := &backchannelTestConn{}
	c := &Client{Conn: &cs2.Conn{Conn: conn}, key: make([]byte, 32)}
	m := NewMux(c)
	m.speakerOnce.Do(func() {}) // Speaker setup is irrelevant to audio writes.
	md := &core.Media{Kind: core.KindAudio, Direction: core.DirectionSendonly,
		Codecs: []*core.Codec{{Name: core.CodecPCMA, ClockRate: 8000}}}
	p0, p1 := &Producer{mux: m}, &Producer{mux: m}
	for _, p := range []*Producer{p0, p1} {
		r := core.NewReceiver(md, md.Codecs[0])
		if err := p.AddTrack(md, md.Codecs[0], r); err != nil {
			t.Fatal(err)
		}
		defer p.Senders[0].Close()
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, p := range []*Producer{p0, p1} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 100; i++ {
				p.Senders[0].Handler(&rtp.Packet{Payload: make([]byte, 320)})
			}
		}()
	}
	close(start)
	wg.Wait()
	if len(conn.sequences) != 200 {
		t.Fatalf("got %d audio writes, want 200", len(conn.sequences))
	}
	for i, seq := range conn.sequences {
		if seq != uint16(i) {
			t.Fatalf("audio write %d has sequence %d", i, seq)
		}
	}
}
