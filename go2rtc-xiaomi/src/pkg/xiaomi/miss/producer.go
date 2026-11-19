package miss

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
	"github.com/AlexxIT/go2rtc/pkg/h264/annexb"
	"github.com/AlexxIT/go2rtc/pkg/h265"
	"github.com/pion/rtp"
)

// Mux shares one MISS Client across up to two camera channels (0 and 1).
type Mux struct {
	client  *Client
	dialMu  sync.Mutex
	writeMu sync.Mutex // serialize backchannel writes and client teardown

	mu           sync.Mutex
	closed       bool
	reading      bool
	err          error
	done         chan struct{}
	prods        map[*Producer]struct{}
	probing      [2]chan *Packet
	startedMask  uint8
	audioMask    uint8
	quality      [2]string
	lensTagKnown bool
	lensTag      uint32
	lensChannel  uint8

	readOnce    sync.Once
	speakerOnce sync.Once
	speakerErr  error
}

type Producer struct {
	core.Connection
	mux       *Mux
	channel   uint8
	wantAudio bool

	mu       sync.Mutex
	active   bool
	stopped  bool
	audioTS  uint32
	done     chan struct{}
	doneOnce sync.Once
}

func Dial(rawURL string) (core.Producer, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if _, err = ParseChannel(u.Query().Get("channel")); err != nil {
		return nil, err
	}

	client, err := NewClient(rawURL)
	if err != nil {
		return nil, err
	}

	m := NewMux(client)
	prod, err := m.Open(rawURL)
	if err != nil {
		_ = m.Close()
		return nil, err
	}
	return prod, nil
}

func NewMux(client *Client) *Mux {
	return &Mux{
		client: client,
		done:   make(chan struct{}),
		prods:  make(map[*Producer]struct{}),
	}
}

func (m *Mux) Closed() bool {
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	return closed
}

// ParseChannel preserves channel=2 and channel=1 as the second lens (index 1).
func ParseChannel(value string) (uint8, error) {
	switch value {
	case "", "0":
		return 0, nil
	case "1", "2":
		return 1, nil
	default:
		return 0, fmt.Errorf("xiaomi: unsupported channel %q", value)
	}
}

func (m *Mux) Open(rawURL string) (core.Producer, error) {
	return m.open(rawURL)
}

func (m *Mux) open(rawURL string) (*Producer, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	query := u.Query()
	channel, err := ParseChannel(query.Get("channel"))
	if err != nil {
		return nil, err
	}
	quality := query.Get("subtype")
	wantAudio := query.Get("audio") != "0"

	m.dialMu.Lock()
	defer m.dialMu.Unlock()

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, io.EOF
	}
	if m.client.isDafang() && len(m.prods) > 0 {
		m.mu.Unlock()
		return nil, errors.New("xiaomi: model does not support dual-channel")
	}

	alreadyStarted := m.startedMask&(1<<channel) != 0
	otherStarted := m.startedMask&(1<<(channel^1)) != 0
	q := m.quality
	if !alreadyStarted {
		q[channel] = quality
	}
	prevAudioMask := m.audioMask
	aMask := prevAudioMask
	if wantAudio {
		aMask |= 1 << channel
	}
	reading := m.reading
	lensKnown := m.lensTagKnown
	m.mu.Unlock()

	var probeCh chan *Packet
	var attached bool
	if reading || otherStarted || alreadyStarted {
		if !reading {
			m.ensureReader()
		}
		probeCh = make(chan *Packet, 256)
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, io.EOF
		}
		m.probing[channel] = probeCh
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			if m.probing[channel] == probeCh {
				m.probing[channel] = nil
			}
			m.mu.Unlock()
			if !attached {
				_ = m.closeIfUnused()
			}
		}()
	}

	if !alreadyStarted {
		if otherStarted && m.client.usesLensTags() && !lensKnown {
			return nil, errors.New("xiaomi: lens tag unknown before dual-channel start")
		}
		if err = m.startMedia(channel, otherStarted, q, aMask != 0); err != nil {
			return nil, err
		}
		m.mu.Lock()
		m.startedMask |= 1 << channel
		m.quality[channel] = quality
		m.audioMask = aMask
		m.mu.Unlock()
	} else if wantAudio && prevAudioMask == 0 {
		if err = m.startMedia(channel, otherStarted, q, true); err != nil {
			return nil, err
		}
		m.mu.Lock()
		m.audioMask = aMask
		m.mu.Unlock()
	}

	medias, err := m.probe(channel, wantAudio, probeCh)
	if err != nil {
		return nil, err
	}

	p := &Producer{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "xiaomi/miss",
			Protocol:   m.client.Protocol(),
			RemoteAddr: m.client.RemoteAddr().String(),
			UserAgent:  m.client.Version(),
			Medias:     medias,
		},
		mux:       m,
		channel:   channel,
		wantAudio: wantAudio,
		done:      make(chan struct{}),
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, io.EOF
	}
	m.prods[p] = struct{}{}
	attached = true
	m.mu.Unlock()

	return p, nil
}

func (m *Mux) probe(channel uint8, audio bool, probeCh <-chan *Packet) ([]*core.Media, error) {
	var read func() (*Packet, error)
	if probeCh == nil {
		_ = m.client.SetDeadline(time.Now().Add(15 * time.Second))
		defer func() {
			_ = m.client.SetDeadline(time.Time{})
		}()
		read = func() (*Packet, error) {
			for {
				pkt, err := m.client.ReadPacket()
				if err != nil {
					return nil, err
				}
				if pkt.CodecID == codecH264 || pkt.CodecID == codecH265 {
					m.mu.Lock()
					ch, ok := m.resolveVideoChannelLocked(pkt)
					m.mu.Unlock()
					if !ok || ch != channel {
						continue
					}
				}
				return pkt, nil
			}
		}
	} else {
		timer := time.NewTimer(15 * time.Second)
		defer timer.Stop()
		read = func() (*Packet, error) {
			select {
			case pkt := <-probeCh:
				return pkt, nil
			case <-m.done:
				m.mu.Lock()
				err := m.err
				m.mu.Unlock()
				if err == nil {
					err = io.EOF
				}
				return nil, err
			case <-timer.C:
				return nil, os.ErrDeadlineExceeded
			}
		}
	}

	var vcodec, acodec *core.Codec
	for {
		pkt, err := read()
		if err != nil {
			// Missing audio can be tolerated after a probe timeout, but a
			// disconnected or malformed stream must still fail the probe.
			var timeout net.Error
			if vcodec != nil && !m.Closed() && errors.As(err, &timeout) && timeout.Timeout() {
				break
			}
			if vcodec == nil && acodec != nil {
				err = fmt.Errorf("no video: %w", err)
			}
			return nil, fmt.Errorf("xiaomi: probe channel=%d: %w", channel, err)
		}

		switch pkt.CodecID {
		case codecH264:
			if vcodec == nil {
				buf := annexb.EncodeToAVCC(pkt.Payload)
				if h264.NALUType(buf) == h264.NALUTypeSPS {
					vcodec = h264.AVCCToCodec(buf)
				}
			}
		case codecH265:
			if vcodec == nil {
				buf := annexb.EncodeToAVCC(pkt.Payload)
				if h265.NALUType(buf) == h265.NALUTypeVPS {
					vcodec = h265.AVCCToCodec(buf)
				}
			}
		case codecPCMA:
			if audio && acodec == nil {
				acodec = &core.Codec{Name: core.CodecPCMA, ClockRate: pkt.SampleRate()}
			}
		case codecOPUS:
			if audio && acodec == nil {
				acodec = &core.Codec{Name: core.CodecOpus, ClockRate: 48000, Channels: 2}
			}
		}

		if vcodec != nil && (acodec != nil || !audio) {
			break
		}
	}

	medias := []*core.Media{
		{
			Kind:      core.KindVideo,
			Direction: core.DirectionRecvonly,
			Codecs:    []*core.Codec{vcodec},
		},
	}

	if acodec != nil {
		medias = append(medias, &core.Media{
			Kind:      core.KindAudio,
			Direction: core.DirectionRecvonly,
			Codecs:    []*core.Codec{acodec},
		})
		medias = append(medias, &core.Media{
			Kind:      core.KindAudio,
			Direction: core.DirectionSendonly,
			Codecs:    []*core.Codec{acodec.Clone()},
		})
	}

	return medias, nil
}

func (m *Mux) ensureReader() {
	m.readOnce.Do(func() {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return
		}
		m.reading = true
		m.mu.Unlock()
		go m.readLoop()
	})
}

func (m *Mux) readLoop() {
	for {
		_ = m.client.SetDeadline(time.Now().Add(10 * time.Second))
		pkt, err := m.client.ReadPacket()
		if err != nil {
			_ = m.closeWith(err, false)
			return
		}
		m.dispatch(pkt)
	}
}

func (m *Mux) dispatch(pkt *Packet) {
	var isVideo, isAudio bool
	switch pkt.CodecID {
	case codecH264, codecH265:
		isVideo = true
	case codecPCM, codecPCMU, codecPCMA, codecOPUS:
		isAudio = true
	default:
		return
	}

	var probe0, probe1 chan *Packet
	var targets []*Producer

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}

	if isVideo {
		ch, ok := m.resolveVideoChannelLocked(pkt)
		if !ok {
			m.mu.Unlock()
			return
		}
		probe0 = m.probing[ch]
		for p := range m.prods {
			if p.channel == ch {
				targets = append(targets, p)
			}
		}
	} else if isAudio {
		probe0, probe1 = m.probing[0], m.probing[1]
		for p := range m.prods {
			if p.wantAudio {
				targets = append(targets, p)
			}
		}
	}
	m.mu.Unlock()

	if probe0 != nil {
		select {
		case probe0 <- pkt:
		default:
		}
	}
	if probe1 != nil {
		select {
		case probe1 <- pkt:
		default:
		}
	}
	for _, p := range targets {
		p.deliver(pkt)
	}
}

func (m *Mux) resolveVideoChannelLocked(pkt *Packet) (uint8, bool) {
	if m.client != nil && m.client.usesLensTags() {
		// Measured CW500 flags: 0x000EA000 / 0x014EA000 (HD),
		// 0x0006A000 / 0x0146A000 (SD). Quality is not a lens ID.
		tag := (pkt.Flags >> 16) & 0x0140
		return m.channelForLensTagLocked(tag)
	}
	if pkt.ChannelOK {
		return pkt.Channel, true
	}
	// Preserve untagged single-channel devices. Once both channels have
	// started, unidentified video must never be assigned to either lens.
	switch m.startedMask {
	case 1:
		return 0, true
	case 2:
		return 1, true
	default:
		return 0, false
	}
}

func (m *Mux) channelForLensTagLocked(tag uint32) (uint8, bool) {
	if tag != 0 && tag != 0x0140 {
		return 0, false
	}
	if !m.lensTagKnown {
		switch m.startedMask {
		case 1:
			m.lensChannel = 0
		case 2:
			m.lensChannel = 1
		default:
			return 0, false
		}
		m.lensTag = tag
		m.lensTagKnown = true
	}
	if tag == m.lensTag {
		return m.lensChannel, true
	}
	return m.lensChannel ^ 1, true
}

func (m *Mux) release(p *Producer) {
	m.mu.Lock()
	if _, ok := m.prods[p]; !ok {
		m.mu.Unlock()
		return
	}
	delete(m.prods, p)
	m.mu.Unlock()

	_ = m.closeIfUnused()
}

func (m *Mux) startMedia(channel uint8, dual bool, q [2]string, audio bool) error {
	a := "0"
	if audio {
		a = "1"
	}
	if dual {
		return m.client.StartMediaDual(q[0], q[1], a)
	}
	return m.client.StartMedia(strconv.Itoa(int(channel)), q[channel], a)
}

func (m *Mux) Close() error {
	return m.closeWith(nil, true)
}

func (m *Mux) closeWith(err error, stop bool) error {
	return m.close(err, stop, false)
}

func (m *Mux) closeIfUnused() error {
	return m.close(nil, true, true)
}

func (m *Mux) close(err error, stop, onlyIfUnused bool) error {
	m.mu.Lock()
	// Check current ownership and reserve teardown in the same critical
	// section. An earlier empty snapshot must not close a new subscriber.
	if m.closed || onlyIfUnused && (len(m.prods) != 0 || m.probing[0] != nil || m.probing[1] != nil) {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.err = err
	close(m.done)
	stop = stop && m.client != nil && m.startedMask != 0
	prods := m.detachProdsLocked()
	m.mu.Unlock()

	for _, p := range prods {
		p.closeDone()
	}
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	if stop {
		_ = m.client.StopMedia()
	}
	if m.client != nil {
		return m.client.Close()
	}
	return nil
}

func (m *Mux) detachProdsLocked() []*Producer {
	if len(m.prods) == 0 {
		return nil
	}
	prods := make([]*Producer, 0, len(m.prods))
	for p := range m.prods {
		prods = append(prods, p)
	}
	m.prods = make(map[*Producer]struct{})
	return prods
}

func (m *Mux) startSpeaker() error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	if m.Closed() {
		return io.EOF
	}
	m.speakerOnce.Do(func() {
		m.speakerErr = m.client.StartSpeaker()
	})
	return m.speakerErr
}

func (m *Mux) writeAudio(codecID uint32, payload []byte) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	if m.Closed() {
		return io.EOF
	}
	return m.client.WriteAudio(codecID, payload)
}

const timestamp40ms = 48000 * 0.040

func (p *Producer) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Connection.GetTrack(media, codec)
}

func (p *Producer) Start() error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	p.active = true
	p.mu.Unlock()

	p.mux.ensureReader()

	<-p.done

	p.mu.Lock()
	stopped := p.stopped
	p.mu.Unlock()
	if stopped {
		return nil
	}

	p.mux.mu.Lock()
	err := p.mux.err
	p.mux.mu.Unlock()
	return err
}

func (p *Producer) Stop() error {
	p.mu.Lock()
	p.active = false
	p.stopped = true
	p.mu.Unlock()

	p.closeDone()
	p.mux.release(p)

	p.mu.Lock()
	err := p.Connection.Stop()
	p.mu.Unlock()
	return err
}

func (p *Producer) closeDone() {
	p.doneOnce.Do(func() {
		close(p.done)
	})
}

func (p *Producer) deliver(pkt *Packet) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.active || len(p.Receivers) == 0 {
		return
	}

	p.Recv += len(pkt.Payload)

	// TODO: rewrite this
	var name string
	var pkt2 *core.Packet

	switch pkt.CodecID {
	case codecH264, codecH265:
		pkt2 = &core.Packet{
			Header: rtp.Header{
				SequenceNumber: uint16(pkt.Sequence),
				Timestamp:      TimeToRTP(pkt.Timestamp, 90000),
			},
			Payload: annexb.EncodeToAVCC(pkt.Payload),
		}
		if pkt.CodecID == codecH264 {
			name = core.CodecH264
		} else {
			name = core.CodecH265
		}
	case codecPCMA:
		name = core.CodecPCMA
		pkt2 = &core.Packet{
			Header: rtp.Header{
				Version:        2,
				Marker:         true,
				SequenceNumber: uint16(pkt.Sequence),
				Timestamp:      p.audioTS,
			},
			Payload: pkt.Payload,
		}
		p.audioTS += uint32(len(pkt.Payload))
	case codecOPUS:
		name = core.CodecOpus
		pkt2 = &core.Packet{
			Header: rtp.Header{
				Version:        2,
				Marker:         true,
				SequenceNumber: uint16(pkt.Sequence),
				Timestamp:      p.audioTS,
			},
			Payload: pkt.Payload,
		}
		// known cameras sends packets with 40ms long
		p.audioTS += timestamp40ms
	}

	for _, recv := range p.Receivers {
		if recv.Codec.Name == name {
			recv.WriteRTP(pkt2)
			break
		}
	}
}

// TimeToRTP convert time in milliseconds to RTP time
func TimeToRTP(timeMS, clockRate uint64) uint32 {
	return uint32(timeMS * clockRate / 1000)
}
