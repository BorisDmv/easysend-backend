package room

import (
	"encoding/json"
	"testing"
	"time"
)

func next(t *testing.T, p *Peer) Message {
	t.Helper()
	select {
	case data, ok := <-p.Outbox():
		if !ok {
			t.Fatalf("outbox of %s closed", p.ID)
		}
		var m Message
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		return m
	case <-time.After(time.Second):
		t.Fatalf("no message for %s", p.ID)
	}
	return Message{}
}

func TestNormalizeCode(t *testing.T) {
	cases := map[string]string{"482910": "482-910", "482-910": "482-910", "482 910": "482-910"}
	for in, want := range cases {
		if got, ok := NormalizeCode(in); !ok || got != want {
			t.Errorf("NormalizeCode(%q) = %q, %v; want %q", in, got, ok, want)
		}
	}
	for _, bad := range []string{"", "48291", "4829101", "48a-910", "../etc"} {
		if _, ok := NormalizeCode(bad); ok {
			t.Errorf("NormalizeCode(%q) accepted invalid code", bad)
		}
	}
}

func TestRelayRoutingAndLeave(t *testing.T) {
	h := NewHub(time.Minute, 4)
	info, _ := h.Create()
	a, _ := h.Join(info.Code, "Blue Fox", "macOS")
	if m := next(t, a); m.Type != TypeWelcome {
		t.Fatalf("a got %q, want welcome", m.Type)
	}
	b, _ := h.Join(info.Code, "Swift Falcon", "Android")
	next(t, b) // welcome
	if m := next(t, a); m.Type != TypePeerJoined {
		t.Fatalf("a got %q, want peer-joined", m.Type)
	}

	// Client-supplied From is overwritten so peers cannot impersonate each other.
	if err := h.Relay(info.Code, a.ID, Message{Type: "offer", From: "spoofed", To: b.ID, Data: json.RawMessage(`{"sdp":"x"}`)}); err != nil {
		t.Fatal(err)
	}
	if m := next(t, b); m.Type != "offer" || m.From != a.ID || string(m.Data) != `{"sdp":"x"}` {
		t.Fatalf("b got %+v", m)
	}
	if len(a.Outbox()) != 0 {
		t.Fatal("sender received its own directed message")
	}

	if err := h.Relay(info.Code, b.ID, Message{Type: TypeWelcome}); err != ErrReservedType {
		t.Fatalf("reserved type: got %v", err)
	}
	if err := h.Relay(info.Code, b.ID, Message{Type: "offer", To: "nobody"}); err != ErrPeerNotFound {
		t.Fatalf("unknown target: got %v", err)
	}

	h.Leave(info.Code, b.ID)
	if m := next(t, a); m.Type != TypePeerLeft {
		t.Fatalf("a got %q, want peer-left", m.Type)
	}
}

func TestRoomCapacity(t *testing.T) {
	h := NewHub(time.Minute, 2)
	info, _ := h.Create()
	h.Join(info.Code, "a", "")
	h.Join(info.Code, "b", "")
	if _, err := h.Join(info.Code, "c", ""); err != ErrFull {
		t.Fatalf("got %v, want ErrFull", err)
	}
}

func TestSlowPeerIsEvicted(t *testing.T) {
	h := NewHub(time.Minute, 4)
	info, _ := h.Create()
	a, _ := h.Join(info.Code, "a", "")
	b, _ := h.Join(info.Code, "b", "")
	next(t, a) // welcome
	next(t, a) // peer-joined

	for range sendBuffer + 1 {
		h.Relay(info.Code, a.ID, Message{Type: "chunk"})
	}
	if m := next(t, a); m.Type != TypePeerLeft {
		t.Fatalf("a got %q, want peer-left", m.Type)
	}
	if got, _ := h.Info(info.Code); got.Peers != 1 {
		t.Fatalf("room has %d peers, want 1", got.Peers)
	}
	for range b.Outbox() {
		// Drains buffered messages; loop ends only because the outbox was closed.
	}
}

func TestSweepRemovesOnlyExpiredEmptyRooms(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	h := NewHub(10*time.Minute, 4)
	h.now = func() time.Time { return now }

	empty, _ := h.Create()
	occupied, _ := h.Create()
	h.Join(occupied.Code, "a", "")

	now = now.Add(9 * time.Minute)
	if n := h.Sweep(); n != 0 {
		t.Fatalf("swept %d rooms before TTL", n)
	}
	now = now.Add(2 * time.Minute)
	if n := h.Sweep(); n != 1 {
		t.Fatalf("swept %d rooms, want 1", n)
	}
	if _, ok := h.Info(empty.Code); ok {
		t.Fatal("expired empty room still exists")
	}
	if _, ok := h.Info(occupied.Code); !ok {
		t.Fatal("occupied room was swept")
	}
}
