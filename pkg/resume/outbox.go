// Package resume keeps the per-player server state that makes reconnects
// lossless: outbound frames for replay and inbound dedupe. It lives on the
// game node, not the gateway, so a gateway crash loses nothing.
//
// A Session is owned by its actor and is not safe for concurrent use.
package resume

// Stamp assigns a frame its seq and returns the wire bytes to store and send.
type Stamp func(seq uint64) []byte

// Session is a fixed-size ring of sent frames plus client request dedupe.
type Session struct {
	frames  [][]byte
	start   uint64 // The first seq this session issued.
	nextSeq uint64 // Seq assigned to the next pushed frame.

	lastClientSeq uint64
	lastResponse  []byte
}

// New creates a session retaining up to capacity frames.
func New(capacity int) *Session { return NewAt(capacity, 1) }

// NewAt creates a session whose seqs begin at start. A later session for the
// same player must start beyond every seq an earlier one issued, or a client's
// old last_seq would match frames it never saw and skip its snapshot.
func NewAt(capacity int, start uint64) *Session {
	if capacity <= 0 {
		capacity = 256
	}
	return &Session{frames: make([][]byte, capacity), start: start, nextSeq: start}
}

// Push assigns the next seq, retains the frame and returns it.
func (s *Session) Push(stamp Stamp) (uint64, []byte) {
	seq := s.nextSeq
	frame := stamp(seq)
	s.frames[seq%uint64(len(s.frames))] = frame
	s.nextSeq++
	return seq, frame
}

// NextSeq is the seq the next pushed frame will get.
func (s *Session) NextSeq() uint64 { return s.nextSeq }

// Replay returns every frame after lastSeq. ok is false when the client's
// position is outside the window (or ahead of the server, which means the
// session was rebuilt), and the caller must send a full snapshot.
func (s *Session) Replay(lastSeq uint64) (frames [][]byte, ok bool) {
	last := s.nextSeq - 1
	if lastSeq > last || lastSeq+1 < s.start {
		return nil, false
	}
	capacity := uint64(len(s.frames))
	if last-lastSeq > capacity {
		return nil, false
	}
	frames = make([][]byte, 0, last-lastSeq)
	for seq := lastSeq + 1; seq <= last; seq++ {
		frames = append(frames, s.frames[seq%capacity])
	}
	return frames, true
}

// Verdict says what to do with an inbound client_seq.
type Verdict int

const (
	Process     Verdict = iota // New request: handle it.
	ResendLast                 // Retry of the latest request: resend its response.
	DropStale                  // Older than the latest: already answered, ignore.
)

// CheckClientSeq classifies an inbound request. Sequence 0 opts out of dedupe
// and is only valid for messages that are safe to repeat, such as heartbeats.
func (s *Session) CheckClientSeq(clientSeq uint64) (Verdict, []byte) {
	switch {
	case clientSeq == 0 || clientSeq > s.lastClientSeq:
		return Process, nil
	case clientSeq == s.lastClientSeq:
		return ResendLast, s.lastResponse
	default:
		return DropStale, nil
	}
}

// ResetDedupe forgets client requests. A new login is a new client, possibly on
// another device, with its own counter.
func (s *Session) ResetDedupe() { s.lastClientSeq, s.lastResponse = 0, nil }

// Commit records the response for clientSeq, for a later retry.
func (s *Session) Commit(clientSeq uint64, response []byte) {
	if clientSeq == 0 {
		return
	}
	s.lastClientSeq = clientSeq
	s.lastResponse = response
}
