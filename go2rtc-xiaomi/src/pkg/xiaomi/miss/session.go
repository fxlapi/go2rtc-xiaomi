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
	// p27: one connection PER LENS for dual cameras. Field tests 2026-11-19
	// (p25/p26) proved the firmware silently ignores every attempt to add
	// ch1 to a connection where ch0 was configured first — single+dual
	// back-to-back, dual as the very first command, dual on an established
	// ch0 stream: ch1 never delivers video. And after any dual merge, ch1
	// waits for its next IDR (~60s GOP) before showing a picture. The only
	// fast path for ch1 is a fresh single-channel start on its own
	// connection, which is also how the camera worked before the
	// single-connection dual patch.
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

// startMedia sends the appropriate VideoStart command. If only one channel is
// active, it sends a single-channel command. If both channels are active, it
// sends a dual-channel command.
func (s *session) startMedia(channel uint8, quality, audio string) error {
	s.mu.Lock()
	ch := int(channel)
	if ch > 1 {
		ch = 0
	}

	// Already started this channel.
	if s.startedMask&(1<<ch) != 0 {
		s.mu.Unlock()
		// p27 watchdog: if this per-lens session's channel never delivers
		// video (command silently ignored by the firmware), retry the
		// single start once instead of leaving the lens dead.
		if s.client.Model() == "isa.camera.cw501d" {
			go s.retryChannelIfDead(uint8(ch), quality, audio)
		}
		return nil
	}

	s.quality[ch] = quality
	other := ch ^ 1
	otherStarted := s.startedMask&(1<<other) != 0
	s.mu.Unlock()

	// Serialize media starts. The camera treats each video-start command as a
	// FULL media config, so two commands arriving within milliseconds (page
	// auto-play starts both lenses at once) can clobber each other. When the
	// other channel has been started, wait until it is actually delivering
	// video before touching the media config again.
	if otherStarted {
		s.awaitChannelVideo(uint8(other), 8*time.Second)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Re-check after waiting: the other dial may have started us meanwhile.
	if s.startedMask&(1<<ch) != 0 {
		return nil
	}
	s.quality[ch] = quality

	// p27: sessions are per-lens (see sessionKey), so each connection starts
	// exactly one channel with a single command — the only path the firmware
	// honors with an immediate IDR. No dual/merge commands on startup.
	if s.client.Model() == "isa.camera.cw501d" && s.startedMask == 0 {
		log.Printf("[xiaomi] p27 per-lens session: starting ch%d only", ch)
	}

	// If the other channel is already started, upgrade to dual-channel.
	if s.startedMask&(1<<other) != 0 {
		// CW501D field tests (2026-11-19 14:10-16:34, p20/p21/p23):
		//
		// - ch1 playing + dual command -> ch0 joins fine (only verified
		//   way to add ch0). ch0 shows ~20s of broken HEVC (missing
		//   reference frames) before valid frames; the probe deadline is
		//   extended (producer.go) to cover that.
		// - ch0 playing + dual command -> ch1 NEVER joins (probe times
		//   out even at 35s). The dual command alone cannot add ch1 in
		//   this direction.
		// - ch0 playing + single "1" command (videoquality:-1) -> ch0
		//   video STOPS, ch1 starts. The session survives the gap.
		//
		// Hence the three-step dance for ch1 joining while ch0 plays:
		// single "1" (stops ch0, starts ch1) -> wait for ch1 to produce
		// frames -> dual command (the only verified way to add ch0 back).
		if ch == 1 {
			if err := s.client.StartMedia("1", quality, audio); err != nil {
				return err
			}
			// ch0 video is stopped by the single command; only ch1 is
			// live until the dual command below re-adds it.
			s.quality[1] = quality
			s.startedMask = 1 << 1
			// Wait WITHOUT holding the lock: dispatch takes s.mu on
			// every packet, and a held lock would stall the reader.
			s.mu.Unlock()
			time.Sleep(3 * time.Second)
			s.mu.Lock()
		}
		err := s.client.StartMediaDual(
			s.quality[0], s.quality[1], audio,
		)
		if err != nil {
			return err
		}
		s.startedMask |= 1<<0 | 1<<1
		return nil
	}

	// Single channel start.
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

// retryChannelIfDead is the p27 safety net: if this per-lens session's
// channel never delivers video (firmware silently ignored the start
// command), re-send the single-channel start once. Only one retry runs at
// a time; a healthy session returns immediately.
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
	log.Printf("[xiaomi] p27: ch%d has no video, retrying single start", ch)
	if err := s.client.StartMedia(itoaCh(ch), quality, audio); err != nil {
		log.Printf("[xiaomi] p27: retry start failed: %v", err)
		return
	}
	log.Printf("[xiaomi] p27: retry start done for ch%d", ch)
}

// itoaCh renders a channel number as its command string ("0" or "1").
func itoaCh(ch uint8) string {
	if ch == 1 {
		return "1"
	}
	return "0"
}

// worker is the read loop that dispatches packets to streams.
func (s *session) worker() {
	for {
		_ = s.client.SetDeadline(time.Now().Add(10 * time.Second))

		pkt, err := s.client.ReadPacket()
		if err != nil {
			s.shutdown()
			return
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
