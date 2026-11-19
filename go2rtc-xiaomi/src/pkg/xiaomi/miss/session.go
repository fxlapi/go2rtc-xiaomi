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
		// p26 watchdog: the startup dual command may have been silently
		// ignored by the firmware (p25 field test 17:38). If a ch1 viewer
		// shows up and ch1 still has no video, recover with the verified
		// three-step dance instead of leaving the lens dead.
		if s.client.Model() == "isa.camera.cw501d" && ch == 1 {
			go s.recoverCh1IfDead()
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

	// p26: dual-lens cameras get the dual command as the VERY FIRST media
	// command on a fresh connection — no single start before it.
	//
	// Field evidence so far:
	// - 2026-11-19 17:38 (p25): single ch0 + immediate dual -> ch1 probe
	//   timed out (35s, no video). So "single first, then dual" does NOT
	//   work even back-to-back.
	// - 2026-11-18: the only verified single-connection dual-lens success
	//   was a simultaneous two-stream open, where the dual command may have
	//   landed before the camera fully committed to single-channel mode.
	// p26 tests the remaining untested variant: configure both channels
	// with the first command, before any single-channel mode exists.
	if s.client.Model() == "isa.camera.cw501d" && s.startedMask == 0 {
		if s.quality[other] == "" {
			s.quality[other] = quality
		}
		err := s.client.StartMediaDual(s.quality[0], s.quality[1], audio)
		log.Printf("[xiaomi] p26 startup dual: first cmd ch%d q0=%s q1=%s audio=%s err=%v",
			ch, s.quality[0], s.quality[1], audio, err)
		if err == nil {
			s.startedMask = 1<<0 | 1<<1
			return nil
		}
		log.Printf("[xiaomi] p26 startup dual failed, falling back to single ch%d: %v", ch, err)
		// Fall through to the single-channel start below.
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

// recoverCh1IfDead is the p26 safety net: if the startup dual command was
// silently ignored by the firmware (ch1 never delivers video), run the
// verified three-step dance — single "1" (stops ch0, starts ch1), wait for
// ch1 frames, then the dual command re-adds ch0. Only one recovery runs at
// a time; a healthy session returns immediately.
func (s *session) recoverCh1IfDead() {
	if !s.ch1Recover.CompareAndSwap(false, true) {
		return
	}
	defer s.ch1Recover.Store(false)

	if s.awaitChannelVideo(1, 12*time.Second) {
		return
	}

	s.mu.Lock()
	if s.startedMask != 1<<0|1<<1 || s.quality[1] == "" {
		// Session not in the startup-dual state (or already recovered
		// by someone else); nothing to do here.
		s.mu.Unlock()
		return
	}
	log.Printf("[xiaomi] p26: ch1 has no video after startup dual, running three-step recovery")
	if err := s.client.StartMedia("1", s.quality[1], ""); err != nil {
		log.Printf("[xiaomi] p26: recovery single start failed: %v", err)
		s.mu.Unlock()
		return
	}
	s.startedMask = 1 << 1
	s.mu.Unlock()

	time.Sleep(3 * time.Second)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.client.StartMediaDual(s.quality[0], s.quality[1], ""); err != nil {
		log.Printf("[xiaomi] p26: recovery dual start failed: %v", err)
		return
	}
	s.startedMask = 1<<0 | 1<<1
	log.Printf("[xiaomi] p26: three-step recovery done, both channels restarted")
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

	// If only one stream is active, send all video to it.
	if len(streams) == 1 {
		streams[0].push(pkt)
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
