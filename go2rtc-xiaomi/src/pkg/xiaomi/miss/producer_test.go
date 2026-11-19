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
	"github.com/AlexxIT/go2rtc/pkg/xiaomi/crypto"
)

type muxTestConn struct {
	Conn
	key       []byte
	mu        sync.Mutex
	commands  []uint32
	cmdBodies []string
	onCommand func(cmd uint32, body string)
	closes    int
	packets   chan muxTestPacket
	closed    chan struct{}
	closeOnce sync.Once
}

type muxTestPacket struct {
	header  []byte
	payload []byte
}

func newMuxTestConn(key []byte) *muxTestConn {
	return &muxTestConn{
		key:     key,
		packets: make(chan muxTestPacket, 32),
		closed:  make(chan struct{}),
	}
}

func (c *muxTestConn) Protocol() string { return "cs2+udp" }
func (c *muxTestConn) Version() string  { return "test" }
func (c *muxTestConn) RemoteAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}
}
func (c *muxTestConn) SetDeadline(time.Time) error { return nil }

func (c *muxTestConn) WriteCommand(cmd uint32, data []byte) error {
	if cmd != cmdEncoded {
		return errors.New("unexpected unencrypted command")
	}
	plain, err := crypto.Decode(data, c.key)
	if err != nil {
		return err
	}
	code := binary.BigEndian.Uint32(plain)
	body := string(plain[4:])
	c.mu.Lock()
	c.commands = append(c.commands, code)
	c.cmdBodies = append(c.cmdBodies, body)
	cb := c.onCommand
	c.mu.Unlock()
	if cb != nil {
		cb(code, body)
	}
	return nil
}

func (c *muxTestConn) ReadPacket() ([]byte, []byte, error) {
	select {
	case pkt := <-c.packets:
		return pkt.header, pkt.payload, nil
	case <-c.closed:
		return nil, nil, io.EOF
	}
}

func (c *muxTestConn) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closes++
		c.mu.Unlock()
		close(c.closed)
	})
	return nil
}

func (c *muxTestConn) pushVideo(codec uint32, seq uint32, flags uint32, ch uint8, tagged bool) {
	var raw []byte
	if codec == codecH264 {
		raw = []byte{0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1e}
	} else {
		raw = []byte{0x00, 0x00, 0x00, 0x01, 0x40, 0x01, 0x0c, 0x01}
	}
	enc, err := crypto.Encode(raw, c.key)
	if err != nil {
		panic(err)
	}
	hdr := make([]byte, hdrSize)
	binary.LittleEndian.PutUint32(hdr[4:], codec)
	binary.LittleEndian.PutUint32(hdr[8:], seq)
	binary.LittleEndian.PutUint32(hdr[12:], flags)
	if tagged {
		binary.LittleEndian.PutUint32(hdr[24:], 1)
		hdr[28] = ch
	} else {
		binary.LittleEndian.PutUint32(hdr[24:], 0xffff)
		hdr[28] = 0xff
	}
	c.packets <- muxTestPacket{header: hdr, payload: enc}
}

func (c *muxTestConn) pushAudio(codec uint32, seq uint32) {
	enc, err := crypto.Encode([]byte{0xd5, 0xd5, 0xd5, 0xd5}, c.key)
	if err != nil {
		panic(err)
	}
	hdr := make([]byte, hdrSize)
	binary.LittleEndian.PutUint32(hdr[4:], codec)
	binary.LittleEndian.PutUint32(hdr[8:], seq)
	c.packets <- muxTestPacket{header: hdr, payload: enc}
}

func attachTestProducer(t *testing.T, m *Mux, channel uint8, wantAudio bool) *Producer {
	t.Helper()
	p := &Producer{
		Connection: core.Connection{
			ID: core.NewID(),
			Medias: []*core.Media{
				{
					Kind:      core.KindVideo,
					Direction: core.DirectionRecvonly,
					Codecs:    []*core.Codec{{Name: core.CodecH265, ClockRate: 90000}},
				},
				{
					Kind:      core.KindAudio,
					Direction: core.DirectionRecvonly,
					Codecs:    []*core.Codec{{Name: core.CodecPCMA, ClockRate: 8000}},
				},
			},
		},
		mux:       m,
		channel:   channel,
		wantAudio: wantAudio,
		active:    true,
		done:      make(chan struct{}),
	}
	for _, md := range p.Medias {
		if _, err := p.GetTrack(md, md.Codecs[0]); err != nil {
			t.Fatal(err)
		}
	}
	m.mu.Lock()
	m.prods[p] = struct{}{}
	m.mu.Unlock()
	return p
}

func TestParseChannel(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  uint8
	}{{"", 0}, {"0", 0}, {"1", 1}, {"2", 1}} {
		got, err := ParseChannel(tc.value)
		if err != nil || got != tc.want {
			t.Errorf("ParseChannel(%q) = %d, %v; want %d", tc.value, got, err, tc.want)
		}
	}
	for _, value := range []string{"-1", "3", "invalid"} {
		if _, err := ParseChannel(value); err == nil {
			t.Errorf("ParseChannel(%q) should fail", value)
		}
	}
}

func TestDialRejectsInvalidChannelBeforeConnecting(t *testing.T) {
	if _, err := Dial("xiaomi://127.0.0.1?channel=invalid"); err == nil || err.Error() != `xiaomi: unsupported channel "invalid"` {
		t.Fatalf("expected channel validation before connecting, got %v", err)
	}
}

func TestMuxRejectsSecondSubscriberOnDafang(t *testing.T) {
	m := NewMux(&Client{model: ModelDafang})
	attachTestProducer(t, m, 0, true)
	if _, err := m.Open("xiaomi://127.0.0.1?channel=2"); err == nil {
		t.Fatal("expected dual-channel open to fail on dafang model")
	}
}

func TestMuxSingleSubscriberRejectsOtherLens(t *testing.T) {
	for _, ch := range []uint8{0, 1} {
		m := NewMux(&Client{})
		p := attachTestProducer(t, m, ch, true)
		m.dispatch(&Packet{CodecID: codecH265, Channel: ch ^ 1, ChannelOK: true, Payload: []byte{0, 0, 0, 1, 0x40}})
		if p.Receivers[0].Packets != 0 {
			t.Errorf("channel %d received packet from other lens", ch)
		}
	}
}

func TestMuxRemainingSubscriberRejectsClosedLens(t *testing.T) {
	m := NewMux(&Client{})
	m.startedMask = 3
	p0 := attachTestProducer(t, m, 0, true)
	p1 := attachTestProducer(t, m, 1, true)
	_ = p1.Stop()

	m.dispatch(&Packet{CodecID: codecH265, Channel: 1, ChannelOK: true, Payload: []byte{0, 0, 0, 1, 0x40}})
	m.dispatch(&Packet{CodecID: codecH265, ChannelOK: false, Payload: []byte{0, 0, 0, 1, 0x40}})
	if p0.Receivers[0].Packets != 0 {
		t.Fatal("remaining channel 0 received closed or untagged dual-session video")
	}
}

func TestMuxUntaggedSingleChannel(t *testing.T) {
	for _, ch := range []uint8{0, 1} {
		m := NewMux(&Client{})
		m.startedMask = 1 << ch
		p := attachTestProducer(t, m, ch, true)
		m.dispatch(&Packet{CodecID: codecH265, Payload: []byte{0, 0, 0, 1, 0x40}})
		if p.Receivers[0].Packets != 1 {
			t.Fatalf("untagged single-channel video missing from channel %d", ch)
		}
	}
}

func TestMuxKnownChannelsStaySeparate(t *testing.T) {
	m := NewMux(&Client{})
	m.startedMask = 3
	p0 := attachTestProducer(t, m, 0, false)
	p1 := attachTestProducer(t, m, 1, false)
	for i := uint32(0); i < 8; i++ {
		ch := uint8(i % 2)
		m.dispatch(&Packet{CodecID: codecH265, Sequence: i, Channel: ch, ChannelOK: true, Payload: []byte{0, 0, 0, 1, 0x40}})
	}
	if p0.Receivers[0].Packets != 4 || p1.Receivers[0].Packets != 4 {
		t.Fatalf("expected 4 packets per channel, got ch0=%d ch1=%d", p0.Receivers[0].Packets, p1.Receivers[0].Packets)
	}
}

func TestMuxAudioBroadcastRespectsAudioFlag(t *testing.T) {
	m := NewMux(&Client{})
	p0 := attachTestProducer(t, m, 0, true)
	p1 := attachTestProducer(t, m, 1, false)
	m.dispatch(&Packet{CodecID: codecPCMA, Payload: []byte{1, 2, 3}})
	if p0.Receivers[1].Packets != 1 || p1.Receivers[1].Packets != 0 {
		t.Fatalf("unexpected audio delivery: ch0=%d ch1=%d", p0.Receivers[1].Packets, p1.Receivers[1].Packets)
	}
}

func TestMuxCW500CapturedLensFlags(t *testing.T) {
	for _, first := range []uint8{0, 1} {
		m := NewMux(&Client{model: "isa.camera.hlmax"})
		m.startedMask = 1 << first
		p0 := attachTestProducer(t, m, 0, false)
		p1 := attachTestProducer(t, m, 1, false)
		prods := [2]*Producer{p0, p1}

		flags := [2]uint32{0x000EA000, 0x014EA000}
		m.dispatch(&Packet{CodecID: codecH265, Flags: flags[first], Payload: []byte{0, 0, 0, 1, 0x40}})
		if prods[first].Receivers[0].Packets != 1 {
			t.Fatalf("first=%d did not learn lens tag", first)
		}

		m.startedMask = 3
		for _, tc := range []struct {
			flags   uint32
			channel uint8
		}{{0x014EA000, 1}, {0x000EA001, 0}, {0x0006A000, 0}, {0x0146A001, 1}} {
			before := prods[tc.channel].Receivers[0].Packets
			otherBefore := prods[tc.channel^1].Receivers[0].Packets
			pkt := &Packet{CodecID: codecH265, Flags: tc.flags, Payload: []byte{0, 0, 0, 1, 0x40}}
			m.dispatch(pkt)
			if prods[tc.channel].Receivers[0].Packets != before+1 || prods[tc.channel^1].Receivers[0].Packets != otherBefore {
				t.Fatalf("first=%d flags=%x routed to wrong lens", first, tc.flags)
			}
			if pkt.ChannelOK {
				t.Fatal("dispatch must not mutate input packet")
			}
		}

		_ = p1.Stop()
		before := p0.Receivers[0].Packets
		m.dispatch(&Packet{CodecID: codecH265, Flags: flags[1], Payload: []byte{0, 0, 0, 1, 0x40}})
		if p0.Receivers[0].Packets != before {
			t.Fatal("closed lens leaked into remaining subscriber")
		}
	}
}

func TestMuxCW500RejectsUnanchoredOrUnexpectedTag(t *testing.T) {
	m := NewMux(&Client{model: "isa.camera.hlmax"})
	p0 := attachTestProducer(t, m, 0, false)
	p1 := attachTestProducer(t, m, 1, false)
	m.startedMask = 3
	m.dispatch(&Packet{CodecID: codecH265, Flags: 0x000EA000, Payload: []byte{0, 0, 0, 1, 0x40}})
	m.startedMask = 1
	m.dispatch(&Packet{CodecID: codecH265, Flags: 0x004EA000, Payload: []byte{0, 0, 0, 1, 0x40}})
	if m.lensTagKnown || p0.Receivers[0].Packets != 0 || p1.Receivers[0].Packets != 0 {
		t.Fatal("invalid or ambiguous lens tag must not be guessed")
	}
}

func TestCW500ReadsFlagsInsteadOfReservedTail(t *testing.T) {
	key := make([]byte, 32)
	conn := newMuxTestConn(key)
	conn.pushVideo(codecH265, 1, 0x014EA000, 0, true)
	c := &Client{Conn: conn, key: key, model: "isa.camera.hlmax"}
	pkt, err := c.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if pkt.ChannelOK || pkt.Flags != 0x014EA000 {
		t.Fatal("CW500 must retain flags for mux mapping, not trust reserved bytes")
	}
}

func TestMuxDualChannelOpenStartStop(t *testing.T) {
	key := make([]byte, 32)
	conn := newMuxTestConn(key)
	conn.onCommand = func(cmd uint32, body string) {
		if cmd != cmdVideoStart {
			return
		}
		switch body {
		case `{"videoquality":2,"enableaudio":1}`:
			conn.pushVideo(codecH265, 1, 0x000EA000, 0, false)
			conn.pushAudio(codecPCMA, 1)
		case `{"videoquality":2,"videoquality2":1,"enableaudio":1}`:
			conn.pushVideo(codecH265, 2, 0x014EA000, 1, false)
		}
	}

	client := &Client{Conn: conn, key: key, model: "isa.camera.hlmax"}
	m := NewMux(client)

	p0Raw, err := m.Open("xiaomi://127.0.0.1?channel=0&subtype=hd")
	if err != nil {
		t.Fatal(err)
	}
	p0 := p0Raw.(*Producer)

	done0 := make(chan error, 1)
	go func() { done0 <- p0.Start() }()

	// Open channel 1 with audio=0 while channel 0 is active; audio must remain enabled.
	p1Raw, err := m.Open("xiaomi://127.0.0.1?channel=2&subtype=sd&audio=0")
	if err != nil {
		t.Fatal(err)
	}
	p1 := p1Raw.(*Producer)

	done1 := make(chan error, 1)
	go func() { done1 <- p1.Start() }()

	conn.mu.Lock()
	cmds := append([]string(nil), conn.cmdBodies...)
	conn.mu.Unlock()
	if len(cmds) != 2 || cmds[1] != `{"videoquality":2,"videoquality2":1,"enableaudio":1}` {
		t.Fatalf("unexpected commands: %v", cmds)
	}

	if err = p1.Stop(); err != nil {
		t.Fatal(err)
	}
	if <-done1 != nil {
		t.Fatal("stopped producer should return nil")
	}
	if m.Closed() || conn.closes != 0 {
		t.Fatal("closing one channel closed shared client")
	}

	if err = p0.Stop(); err != nil {
		t.Fatal(err)
	}
	if <-done0 != nil {
		t.Fatal("stopped producer should return nil")
	}
	if !m.Closed() || conn.closes != 1 {
		t.Fatal("closing last channel must close shared client once")
	}
	conn.mu.Lock()
	lastCmd := conn.commands[len(conn.commands)-1]
	conn.mu.Unlock()
	if lastCmd != cmdVideoStop {
		t.Fatalf("expected StopMedia on final close, got command %x", lastCmd)
	}

	if _, err = m.Open("xiaomi://127.0.0.1?channel=0"); !errors.Is(err, io.EOF) {
		t.Fatalf("closed mux accepted subscriber: %v", err)
	}
}
