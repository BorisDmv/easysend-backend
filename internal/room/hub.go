// Package room keeps track of pairing rooms and relays signalling messages
// between the peers in each room. Message payloads are opaque to the server.
package room

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"
)

const sendBuffer = 64

var (
	ErrNotFound     = errors.New("room not found")
	ErrFull         = errors.New("room is full")
	ErrPeerNotFound = errors.New("target peer not found")
	ErrReservedType = errors.New("message type is reserved")
	ErrMissingType  = errors.New("message type is required")
)

// Server-generated message types; clients may not send these.
const (
	TypeWelcome    = "welcome"
	TypePeerJoined = "peer-joined"
	TypePeerLeft   = "peer-left"
	TypeError      = "error"
)

// Message is the envelope exchanged over the room socket. Clients set To to
// address a single peer (omit it to broadcast); the server always sets From.
type Message struct {
	Type string          `json:"type"`
	From string          `json:"from,omitempty"`
	To   string          `json:"to,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

type PeerInfo struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Device string `json:"device"`
}

type Peer struct {
	PeerInfo
	send chan []byte
}

// Outbox yields encoded messages for this peer. It is closed when the peer
// leaves or is evicted for not keeping up.
func (p *Peer) Outbox() <-chan []byte { return p.send }

type Info struct {
	Code      string     `json:"code"`
	Peers     int        `json:"peers"`
	MaxPeers  int        `json:"maxPeers"`
	CreatedAt time.Time  `json:"createdAt"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
}

type room struct {
	code       string
	createdAt  time.Time
	emptySince time.Time
	peers      map[string]*Peer
}

type Hub struct {
	mu       sync.Mutex
	rooms    map[string]*room
	ttl      time.Duration
	maxPeers int
	now      func() time.Time
}

// NewHub creates a hub whose rooms are deleted after staying empty for ttl.
func NewHub(ttl time.Duration, maxPeers int) *Hub {
	return &Hub{rooms: map[string]*room{}, ttl: ttl, maxPeers: maxPeers, now: time.Now}
}

func (h *Hub) Create() (Info, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for range 20 {
		code, err := randomCode()
		if err != nil {
			return Info{}, err
		}
		if _, taken := h.rooms[code]; taken {
			continue
		}
		now := h.now()
		r := &room{code: code, createdAt: now, emptySince: now, peers: map[string]*Peer{}}
		h.rooms[code] = r
		return h.infoLocked(r), nil
	}
	return Info{}, errors.New("could not allocate a free room code")
}

func (h *Hub) Info(code string) (Info, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.rooms[code]
	if !ok {
		return Info{}, false
	}
	return h.infoLocked(r), true
}

// Join adds a peer to the room, queues a welcome message (with the existing
// peers) for it and announces it to everyone else.
func (h *Hub) Join(code, name, device string) (*Peer, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.rooms[code]
	if !ok {
		return nil, ErrNotFound
	}
	if len(r.peers) >= h.maxPeers {
		return nil, ErrFull
	}
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	p := &Peer{
		PeerInfo: PeerInfo{ID: id, Name: clean(name, "Anonymous"), Device: clean(device, "Unknown device")},
		send:     make(chan []byte, sendBuffer),
	}
	existing := make([]PeerInfo, 0, len(r.peers))
	for _, other := range r.peers {
		existing = append(existing, other.PeerInfo)
	}
	r.peers[id] = p
	h.deliverLocked(r, p, system(TypeWelcome, map[string]any{"peerId": id, "room": code, "peers": existing}))
	h.broadcastLocked(r, id, system(TypePeerJoined, p.PeerInfo))
	return p, nil
}

// Leave removes the peer, closes its outbox and announces its departure.
func (h *Hub) Leave(code, peerID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if r, ok := h.rooms[code]; ok {
		if p, ok := r.peers[peerID]; ok {
			h.removeLocked(r, p)
		}
	}
}

// Relay forwards a client message to one peer (msg.To) or all other peers.
func (h *Hub) Relay(code, fromID string, msg Message) error {
	switch msg.Type {
	case "":
		return ErrMissingType
	case TypeWelcome, TypePeerJoined, TypePeerLeft, TypeError:
		return ErrReservedType
	}
	to := msg.To
	msg.To = ""
	msg.From = fromID
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.rooms[code]
	if !ok {
		return ErrNotFound
	}
	if _, ok := r.peers[fromID]; !ok {
		return ErrPeerNotFound
	}
	if to == "" {
		h.broadcastLocked(r, fromID, data)
		return nil
	}
	target, ok := r.peers[to]
	if !ok || to == fromID {
		return ErrPeerNotFound
	}
	h.deliverLocked(r, target, data)
	return nil
}

func (h *Hub) SendError(code, peerID, message string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if r, ok := h.rooms[code]; ok {
		if p, ok := r.peers[peerID]; ok {
			h.deliverLocked(r, p, system(TypeError, map[string]string{"message": message}))
		}
	}
}

// Sweep deletes rooms that have been empty for longer than the TTL.
func (h *Hub) Sweep() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	removed := 0
	for code, r := range h.rooms {
		if len(r.peers) == 0 && now.Sub(r.emptySince) > h.ttl {
			delete(h.rooms, code)
			removed++
		}
	}
	return removed
}

func (h *Hub) RunJanitor(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.Sweep()
		}
	}
}

func (h *Hub) infoLocked(r *room) Info {
	info := Info{Code: r.code, Peers: len(r.peers), MaxPeers: h.maxPeers, CreatedAt: r.createdAt}
	if len(r.peers) == 0 {
		exp := r.emptySince.Add(h.ttl)
		info.ExpiresAt = &exp
	}
	return info
}

func (h *Hub) broadcastLocked(r *room, exceptID string, data []byte) {
	for id, p := range r.peers {
		if id != exceptID {
			h.deliverLocked(r, p, data)
		}
	}
}

// deliverLocked never blocks: a peer whose buffer is full is evicted so one
// slow connection cannot stall the whole room.
func (h *Hub) deliverLocked(r *room, p *Peer, data []byte) {
	select {
	case p.send <- data:
	default:
		h.removeLocked(r, p)
	}
}

func (h *Hub) removeLocked(r *room, p *Peer) {
	if r.peers[p.ID] != p {
		return
	}
	delete(r.peers, p.ID)
	close(p.send)
	if len(r.peers) == 0 {
		r.emptySince = h.now()
	}
	h.broadcastLocked(r, "", system(TypePeerLeft, map[string]string{"peerId": p.ID}))
}

// NormalizeCode accepts "482910", "482-910" or "482 910" and returns "482-910".
func NormalizeCode(raw string) (string, bool) {
	var b strings.Builder
	for _, c := range raw {
		switch {
		case c >= '0' && c <= '9':
			b.WriteRune(c)
		case c == '-' || c == ' ':
		default:
			return "", false
		}
	}
	s := b.String()
	if len(s) != 6 {
		return "", false
	}
	return s[:3] + "-" + s[3:], true
}

func system(typ string, data any) []byte {
	raw, _ := json.Marshal(data)
	out, _ := json.Marshal(Message{Type: typ, Data: raw})
	return out
}

func randomCode() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(1_000_000))
	if err != nil {
		return "", err
	}
	s := fmt.Sprintf("%06d", n.Int64())
	return s[:3] + "-" + s[3:], nil
}

func randomID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func clean(s, fallback string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return fallback
	}
	if r := []rune(s); len(r) > 40 {
		s = string(r[:40])
	}
	return s
}
