package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"magicbox/backend/internal/room"
)

func TestTwoPeersSignalThroughRoom(t *testing.T) {
	hub := room.NewHub(time.Minute, 8)
	srv := httptest.NewServer(New(hub, []string{"localhost:5173"}, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer srv.Close()

	res, err := http.Post(srv.URL+"/api/rooms", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var info room.Info
	json.NewDecoder(res.Body).Decode(&info)
	res.Body.Close()
	if res.StatusCode != http.StatusCreated || info.Code == "" {
		t.Fatalf("create room: status %d, info %+v", res.StatusCode, info)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/rooms/" + strings.ReplaceAll(info.Code, "-", "")

	dial := func(name string) (*websocket.Conn, string) {
		c, _, err := websocket.Dial(ctx, wsURL+"?name="+name, nil)
		if err != nil {
			t.Fatal(err)
		}
		var welcome struct {
			Type string
			Data struct{ PeerID string }
		}
		if err := wsjson.Read(ctx, c, &welcome); err != nil || welcome.Type != room.TypeWelcome {
			t.Fatalf("%s welcome: %+v, %v", name, welcome, err)
		}
		return c, welcome.Data.PeerID
	}

	a, aID := dial("A")
	defer a.CloseNow()
	b, bID := dial("B")
	defer b.CloseNow()

	var joined room.Message
	if err := wsjson.Read(ctx, a, &joined); err != nil || joined.Type != room.TypePeerJoined {
		t.Fatalf("a peer-joined: %+v, %v", joined, err)
	}

	wsjson.Write(ctx, a, room.Message{Type: "offer", To: bID, Data: json.RawMessage(`{"sdp":"v=0"}`)})
	var got room.Message
	if err := wsjson.Read(ctx, b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Type != "offer" || got.From != aID || string(got.Data) != `{"sdp":"v=0"}` {
		t.Fatalf("b got %+v", got)
	}

	b.Close(websocket.StatusNormalClosure, "")
	var left room.Message
	if err := wsjson.Read(ctx, a, &left); err != nil || left.Type != room.TypePeerLeft {
		t.Fatalf("a peer-left: %+v, %v", left, err)
	}
}

func TestSocketRejectsUnknownRoomAndForeignOrigin(t *testing.T) {
	hub := room.NewHub(time.Minute, 8)
	srv := httptest.NewServer(New(hub, []string{"localhost:5173"}, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := "ws" + strings.TrimPrefix(srv.URL, "http")

	if _, res, err := websocket.Dial(ctx, base+"/ws/rooms/000000", nil); err == nil || res == nil || res.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown room: err=%v", err)
	}

	info, _ := hub.Create()
	header := http.Header{"Origin": []string{"https://evil.example"}}
	if _, res, err := websocket.Dial(ctx, base+"/ws/rooms/"+info.Code, &websocket.DialOptions{HTTPHeader: header}); err == nil || res == nil || res.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign origin: err=%v", err)
	}
}
