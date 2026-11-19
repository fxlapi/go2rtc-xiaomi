package miss

import (
	"io"
	"log"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// session manages a single MISS client connection that can serve multiple
// streams (channels). For dual-channel cameras, both channel 0 and channel 1
// share the same underlying UDP/TCP session to avoid exhausting the camera's
// limited connection slots.
type session struct {
	client *Client
	key    string // cache key: host|did

	mu      sync.Mutex
	streams map[*stream]struct{}

	// startedMask tracks which channels have been started (bit 0 = ch0, bit 1 = ch1).
	startedMask uint8
	quality     [2]string // remembered quality per channel

	workerOnce sync.Once
	closeOnce  sync.Once

	speakerOnce sync.Once
	speakerErr  error

	// ch1Recover guards the p26 ch1 recovery goroutine (one at a time).
	ch1Recover atomic.Bool

	// sampleN counts remaining post-merge packets to log (p30 diagnostics).
	sampleN atomic.Int32
}

// stream represents a single channel's view of a session. It receives
// packets dispatched by the session worker and provides a ReadPacket
// interface compatible with what the Producer expects.
type stream struct {
	session *session
	channel uint8
	ch      chan *Packet

	closeOnce sync.Once
	deadline  atomic.Value // time.Time
	done      chan struct{}
	gotVideo  atomic.Bool // set when a video packet has been pushed
}

var (
	sessionMu sync.Mutex
	sessions  = map[string]*session{}
)

// sessionKey builds a cache key from the URL. Two URLs pointing to the same
// camera (same host and did) will share a session.
func sessionKey(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	key := u.Host
	if did := u.Query().Get("did"); did != "" {
		key += "|" + did
	}
	// p34: back to ONE connection PER LENS (p27 behavior). Packet counters
	// (p29-p33, 2026-11-19) settled it: on a single connection the camera's
	// media config is locked by the first cmdVideoStart — the dual merge as
	// the first command yields ch0-only video with no audio, and ANY later
	// command (merge or ch1-add) bursts the new channel for ~1s then the
	// camera stops sending it. Dual-lens on one connection is not achievable
	// with this firmware's command set. Per-lens connections cost 2 slots
	// while both lenses are watched; the slots free automatically when the
	// streams go idle, leaving room for the Mi Home app.
	if ch := u.Query().Get("channel"); ch == "1" || ch == "2" {
		key += "|ch1"
	}
	return key, nil
}

// getOrCreateSession returns an existing session for the camera or creates a
// new one. Dafang-like models that don't support dual-channel always get a
// fresh client (no session sharing).
func getOrCreateSession(rawURL string) (*session, error) {
	key, err := sessionKey(rawURL)
	if err != nil {
		return nil, err
	}

	sessionMu.Lock()
	if s, ok := sessions[key]; ok {
		// Reuse existing session only if the underlying client supports
		// dual-channel (not dafang-like).
		if !s.client.IsDafangLike() {
			sessionMu.Unlock()
			return s, nil
		}
	}
	sessionMu.Unlock()

	client, err := NewClient(rawURL)
	if err != nil {
		return nil, err
	}

	// Dafang-like models: no session sharing, return standalone session.
	if client.IsDafangLike() {
		return &session{
			client:  client,
			key:     key,
			streams: make(map[*stream]struct{}),
		}, nil
	}

	s := &session{
		client:  client,
		key:     key,
		streams: make(map[*stream]struct{}),
	}

	sessionMu.Lock()
	if existing, ok := sessions[key]; ok {
		// Another goroutine created it between our check and now.
		sessionMu.Unlock()
		_ = client.Close()
		return existing, nil
	}
	sessions[key] = s
	sessionMu.Unlock()

	return s, nil
}

// openStream creates a new stream for the given channel on this session.
func (s *session) openStream(channel uint8) *stream {
	st := &stream{
		session: s,
		channel: channel,
		ch:      make(chan *Packet, 100),
		done:    make(chan struct{}),
	}
	st.deadline.Store(time.Time{})

	s.mu.Lock()
	s.streams[st] = struct{}{}
	s.mu.Unlock()

	s.workerOnce.Do(func() {
		go s.worker()
	})

	return st
}

// startMedia sends the appropriate VideoStart command. With per-lens
// sessions (p34) each connection starts exactly one channel.
func (s *session) startMedia(channel uint8, quality, audio string) error {
	s.mu.Lock()
	ch := int(channel)
	if ch > 1 {
		ch = 0
	}

	// Already started this channel.
	if s.startedMask&(1<<ch) != 0 {
		s.mu.Unlock()
		// Watchdog: if this channel never delivers video (a command
		// silently ignored by the firmware), recover instead of leaving
		// the lens dead.
		if s.client.Model() == "isa.camera.cw501d" {
			go s.retryChannelIfDead(uint8(ch), quality, audio)
		}
		return nil
	}

	s.quality[ch] = quality
	defer s.mu.Unlock()

	// p34: per-lens sessions (see sessionKey) — each connection starts
	// exactly one channel with a single command, the only path this firmware
	// honors with a sustained stream and an immediate IDR.
	if s.client.Model() == "isa.camera.cw501d" {
		log.Printf("[xiaomi] p34 per-lens session: starting ch%d only", ch)
	}
	chStr := "0"
	if channel == 1 {
		chStr = "1"
	}
	if err := s.client.StartMedia(chStr, quality, audio); err != nil {
		return err
	}
	s.startedMask |= 1 << ch
	return nil
}

// awaitChannelVideo waits until some stream of the given channel has received
// a video packet, or until the timeout elapses. Returns true if video was seen.
func (s *session) awaitChannelVideo(ch uint8, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		for st := range s.streams {
			if st.channel == ch && st.gotVideo.Load() {
				s.mu.Unlock()
				return true
			}
		}
		s.mu.Unlock()
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// retryChannelIfDead is the safety net: if this per-lens session's channel
// never delivers video (firmware silently ignored the start command), re-send
// the single-channel start once. Only one retry runs at a time; a healthy
// session returns immediately.
func (s *session) retryChannelIfDead(ch uint8, quality, audio string) {
	if !s.ch1Recover.CompareAndSwap(false, true) {
		return
	}
	defer s.ch1Recover.Store(false)

	if s.awaitChannelVideo(ch, 12*time.Second) {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.startedMask != 1<<ch {
		// Session state changed meanwhile; nothing to retry here.
		return
	}
	log.Printf("[xiaomi] p34: ch%d has no video, retrying single start", ch)
	if err := s.client.StartMedia(itoaCh(ch), quality, audio); err != nil {
		log.Printf("[xiaomi] p34: retry start failed: %v", err)
		return
	}
	log.Printf("[xiaomi] p34: retry start done for ch%d", ch)
}

// itoaCh renders a channel number as its command string ("0" or "1").
func itoaCh(ch uint8) string {
	if ch == 1 {
		return "1"
	}
	return "0"
}

// sampleAfterMerge arms the worker to log the next 20 video packets after a
// dual merge so we can see exactly what the camera sends for each lens
// (p30 diagnostics). The worker does the actual logging (single reader).
func (s *session) sampleAfterMerge() {
	s.sampleN.Store(20)
}

// worker is the read loop that dispatches packets to streams.
func (s *session) worker() {
	// p29 diagnostics: periodic per-channel packet counts.
	var c0, c1, aud, vcnt uint32
	go func() {
		for range time.Tick(5 * time.Second) {
			s.mu.Lock()
			n := len(s.streams)
			mask := s.startedMask
			s.mu.Unlock()
			log.Printf("[xiaomi] p29 stats: ch0=%d ch1=%d audio=%d streams=%d startedMask=%d",
				c0, c1, aud, n, mask)
		}
	}()
	for {
		_ = s.client.SetDeadline(time.Now().Add(10 * time.Second))

		pkt, err := s.client.ReadPacket()
		if err != nil {
			s.shutdown()
			return
		}

		if isAudioCodec(pkt.CodecID) {
			aud++
		} else if pkt.Channel == 1 {
			c1++
		} else {
			c0++
		}
		if !isAudioCodec(pkt.CodecID) {
			vcnt++
			if vcnt%300 == 0 {
				log.Printf("[xiaomi] p29 sample: seq=%d flags=%#08x ch=%d chOK=%v codec=%08x",
					pkt.Sequence, pkt.Flags, pkt.Channel, pkt.ChannelOK, pkt.CodecID)
			}
			if n := s.sampleN.Add(-1); n >= 0 {
				log.Printf("[xiaomi] p30 post-merge sample: seq=%d flags=%#08x ch=%d chOK=%v codec=%08x len=%d",
					pkt.Sequence, pkt.Flags, pkt.Channel, pkt.ChannelOK, pkt.CodecID, len(pkt.Payload))
			}
		}

		s.dispatch(pkt)
	}
}

// dispatch routes a packet to the appropriate stream(s).
func (s *session) dispatch(pkt *Packet) {
	s.mu.Lock()
	if len(s.streams) == 0 {
		s.mu.Unlock()
		return
	}

	streams := make([]*stream, 0, len(s.streams))
	for st := range s.streams {
		streams = append(streams, st)
	}
	s.mu.Unlock()

	// Audio packets are broadcast to all streams.
	if isAudioCodec(pkt.CodecID) {
		for _, st := range streams {
			st.push(pkt)
		}
		return
	}

	// If only one stream is active, send all video to it. p27: when the
	// packet channel is known and differs from the stream's channel, still
	// deliver the packet (single-consumer sessions get whatever the camera
	// sends) but do NOT mark gotVideo — the watchdog must not mistake
	// another lens's frames for this lens delivering video.
	if len(streams) == 1 {
		st := streams[0]
		if pkt.ChannelOK && pkt.Channel != st.channel {
			st.pushNoVideo(pkt)
		} else {
			st.push(pkt)
		}
		return
	}

	// Multiple streams: route by channel.
	ch := s.pickChannel(pkt)
	for _, st := range streams {
		if st.channel == ch {
			st.push(pkt)
		}
	}
}

// pickChannel determines which channel a video packet belongs to.
func (s *session) pickChannel(pkt *Packet) uint8 {
	// Primary: use the channel field parsed from the packet header.
	if pkt.ChannelOK {
		return pkt.Channel
	}
	// Fallback: default to channel 0.
	return 0
}

// startSpeaker starts the speaker on the session (once).
func (s *session) startSpeaker() error {
	s.speakerOnce.Do(func() {
		s.speakerErr = s.client.StartSpeaker()
	})
	return s.speakerErr
}

// writeAudio sends audio data to the camera.
func (s *session) writeAudio(codecID uint32, payload []byte) error {
	return s.client.WriteAudio(codecID, payload)
}

// speakerCodec returns the speaker codec for the camera model.
func (s *session) speakerCodec() uint32 {
	return s.client.SpeakerCodec()
}

// removeStream removes a stream from the session. If no streams remain, the
// session is shut down.
func (s *session) removeStream(st *stream) {
	s.mu.Lock()
	if _, ok := s.streams[st]; !ok {
		s.mu.Unlock()
		return
	}
	delete(s.streams, st)
	empty := len(s.streams) == 0
	s.mu.Unlock()

	st.close()

	if empty {
		s.shutdown()
	}
}

// shutdown tears down the session, closing all streams and the client.
func (s *session) shutdown() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		streams := make([]*stream, 0, len(s.streams))
		for st := range s.streams {
			streams = append(streams, st)
		}
		s.streams = make(map[*stream]struct{})
		s.mu.Unlock()

		for _, st := range streams {
			st.close()
		}

		_ = s.client.Close()

		sessionMu.Lock()
		if sessions[s.key] == s {
			delete(sessions, s.key)
		}
		sessionMu.Unlock()
	})
}

// --- stream methods ---

// ReadPacket reads the next packet for this stream, respecting deadlines.
func (st *stream) ReadPacket() (*Packet, error) {
	deadline, _ := st.deadline.Load().(time.Time)
	if deadline.IsZero() {
		select {
		case pkt := <-st.ch:
			return pkt, nil
		case <-st.done:
			return nil, io.EOF
		}
	}

	d := time.Until(deadline)
	if d <= 0 {
		// Check if there's already a packet available.
		select {
		case pkt := <-st.ch:
			return pkt, nil
		default:
			return nil, &net.OpError{Op: "read", Err: &timeoutError{}}
		}
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case pkt := <-st.ch:
		return pkt, nil
	case <-st.done:
		return nil, io.EOF
	case <-timer.C:
		return nil, &net.OpError{Op: "read", Err: &timeoutError{}}
	}
}

// SetDeadline sets a read deadline for this stream.
func (st *stream) SetDeadline(t time.Time) error {
	st.deadline.Store(t)
	return nil
}

// RemoteAddr returns the remote address of the underlying connection.
func (st *stream) RemoteAddr() net.Addr {
	return st.session.client.RemoteAddr()
}

// Close removes this stream from the session.
func (st *stream) Close() error {
	st.session.removeStream(st)
	return nil
}

func (st *stream) push(pkt *Packet) {
	if !isAudioCodec(pkt.CodecID) {
		st.gotVideo.Store(true)
	}
	select {
	case <-st.done:
		return
	default:
	}
	select {
	case st.ch <- pkt:
	default:
		// Drop packet if the buffer is full.
	}
}

// pushNoVideo delivers the packet without marking the stream as having
// received its own lens's video (p27 watchdog accuracy).
func (st *stream) pushNoVideo(pkt *Packet) {
	select {
	case <-st.done:
		return
	default:
	}
	select {
	case st.ch <- pkt:
	default:
	}
}

func (st *stream) close() {
	st.closeOnce.Do(func() {
		close(st.done)
	})
}

// --- helpers ---

func isAudioCodec(codecID uint32) bool {
	switch codecID {
	case codecPCM, codecPCMU, codecPCMA, codecOPUS:
		return true
	}
	return false
}

// timeoutError implements the net.Error interface for deadline timeouts.
type timeoutError struct{}

func (e *timeoutError) Error() string   { return "i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }
