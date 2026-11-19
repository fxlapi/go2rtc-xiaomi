package miss

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

type probeTestConn struct {
	*muxTestConn
	readErr error
}

func (c *probeTestConn) ReadPacket() ([]byte, []byte, error) {
	select {
	case pkt := <-c.packets:
		return pkt.header, pkt.payload, nil
	default:
		return nil, nil, c.readErr
	}
}

func TestProbeRejectsReadFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"eof", io.EOF},
		{"unexpected-eof", io.ErrUnexpectedEOF},
		{"read-eof", &net.OpError{Op: "read", Net: "udp", Err: io.EOF}},
		{"decode-error", errors.New("invalid encrypted packet")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := make([]byte, 32)
			conn := &probeTestConn{muxTestConn: newMuxTestConn(key), readErr: tc.err}
			conn.pushVideo(codecH265, 1, 0, 0, true)
			m := NewMux(&Client{Conn: conn, key: key})
			m.startedMask = 1
			medias, err := m.probe(0, true, nil)
			if !errors.Is(err, tc.err) || medias != nil {
				t.Fatalf("probe returned %v, %v; want the read error", medias, err)
			}
			if strings.Contains(err.Error(), "no video") {
				t.Fatalf("video was found, but the error says otherwise: %v", err)
			}
		})
	}
}

func TestProbeAllowsMissingAudioOnTimeout(t *testing.T) {
	for _, readErr := range []error{
		os.ErrDeadlineExceeded,
		&net.OpError{Op: "read", Net: "udp", Err: os.ErrDeadlineExceeded},
		fmt.Errorf("wrapped timeout: %w", os.ErrDeadlineExceeded),
	} {
		key := make([]byte, 32)
		conn := &probeTestConn{muxTestConn: newMuxTestConn(key), readErr: readErr}
		conn.pushVideo(codecH265, 1, 0, 0, true)
		m := NewMux(&Client{Conn: conn, key: key})
		m.startedMask = 1
		medias, err := m.probe(0, true, nil)
		if err != nil || len(medias) != 1 || medias[0].Kind != core.KindVideo {
			t.Fatalf("timeout %v: got %v, %v; want video-only media", readErr, medias, err)
		}
	}
}

func TestProbeTimeoutWithoutVideoFails(t *testing.T) {
	key := make([]byte, 32)
	conn := &probeTestConn{muxTestConn: newMuxTestConn(key), readErr: os.ErrDeadlineExceeded}
	conn.pushAudio(codecPCMA, 1)
	m := NewMux(&Client{Conn: conn, key: key})
	medias, err := m.probe(0, true, nil)
	if !errors.Is(err, os.ErrDeadlineExceeded) || medias != nil || !strings.Contains(err.Error(), "no video") {
		t.Fatalf("probe returned %v, %v; want a no-video timeout", medias, err)
	}
}

func TestProbeAudioTracksRespectAudioOption(t *testing.T) {
	for _, codec := range []uint32{codecPCMA, codecOPUS} {
		for _, audioFirst := range []bool{false, true} {
			for _, wantAudio := range []bool{false, true} {
				t.Run(fmt.Sprintf("codec=%d/audio-first=%t/audio=%t", codec, audioFirst, wantAudio), func(t *testing.T) {
					// Audio can arrive from a different lens on the shared connection.
					probe := make(chan *Packet, 2)
					audio := &Packet{CodecID: codec, Payload: []byte{1, 2, 3, 4}}
					video := &Packet{CodecID: codecH265, Payload: []byte{0, 0, 0, 1, 0x40, 1, 0x0c, 1}}
					if audioFirst {
						probe <- audio
						probe <- video
					} else {
						probe <- video
						probe <- audio
					}
					m := NewMux(&Client{})
					medias, err := m.probe(0, wantAudio, probe)
					if err != nil {
						t.Fatal(err)
					}
					want := 1
					if wantAudio {
						want = 3 // video, receive audio, and backchannel
					}
					if len(medias) != want || medias[0].Kind != core.KindVideo {
						t.Fatalf("got %v; want %d media entries", medias, want)
					}
					for _, media := range medias[1:] {
						if !wantAudio || media.Kind != core.KindAudio {
							t.Fatalf("unexpected audio media: %v", media)
						}
					}
				})
			}
		}
	}
}
