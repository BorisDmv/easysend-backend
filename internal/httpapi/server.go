// Package httpapi exposes the room hub over HTTP and WebSocket.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/coder/websocket"

	"magicbox/backend/internal/room"
)

const (
	// Signalling only: offers, answers, ICE candidates and key-exchange messages.
	maxMessageBytes = 64 << 10
	writeTimeout    = 10 * time.Second
	pingInterval    = 30 * time.Second
)

type Server struct {
	hub     *room.Hub
	origins []string
	log     *slog.Logger
}

// New returns the API handler. origins are host patterns (e.g. "localhost:5173")
// allowed for CORS and WebSocket upgrades.
func New(hub *room.Hub, origins []string, log *slog.Logger) http.Handler {
	s := &Server{hub: hub, origins: origins, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.health)
	mux.HandleFunc("POST /api/rooms", s.createRoom)
	mux.HandleFunc("GET /api/rooms/{code}", s.getRoom)
	mux.HandleFunc("GET /ws/rooms/{code}", s.roomSocket)
	return s.withCORS(mux)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) createRoom(w http.ResponseWriter, _ *http.Request) {
	info, err := s.hub.Create()
	if err != nil {
		s.log.Error("create room", "err", err)
		writeError(w, http.StatusInternalServerError, "could not create room")
		return
	}
	s.log.Info("room created", "room", info.Code)
	writeJSON(w, http.StatusCreated, info)
}

func (s *Server) getRoom(w http.ResponseWriter, r *http.Request) {
	code, ok := room.NormalizeCode(r.PathValue("code"))
	if !ok {
		writeError(w, http.StatusBadRequest, "room code must be 6 digits")
		return
	}
	info, ok := s.hub.Info(code)
	if !ok {
		writeError(w, http.StatusNotFound, "room not found")
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) roomSocket(w http.ResponseWriter, r *http.Request) {
	code, ok := room.NormalizeCode(r.PathValue("code"))
	if !ok {
		writeError(w, http.StatusBadRequest, "room code must be 6 digits")
		return
	}
	if _, ok := s.hub.Info(code); !ok {
		writeError(w, http.StatusNotFound, "room not found")
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: s.origins})
	if err != nil {
		s.log.Warn("websocket accept failed", "err", err)
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(maxMessageBytes)

	q := r.URL.Query()
	peer, err := s.hub.Join(code, q.Get("name"), q.Get("device"))
	if err != nil {
		status := websocket.StatusPolicyViolation
		if errors.Is(err, room.ErrFull) {
			status = websocket.StatusTryAgainLater
		}
		conn.Close(status, err.Error())
		return
	}
	log := s.log.With("room", code, "peer", peer.ID)
	log.Info("peer joined", "name", peer.Name, "device", peer.Device)

	ctx, cancel := context.WithCancel(r.Context())
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		writeLoop(ctx, conn, peer)
	}()

	s.readLoop(ctx, conn, code, peer)
	s.hub.Leave(code, peer.ID)
	cancel()
	<-writerDone
	log.Info("peer left")
}

func (s *Server) readLoop(ctx context.Context, conn *websocket.Conn, code string, peer *room.Peer) {
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			s.hub.SendError(code, peer.ID, "only text JSON messages are supported")
			continue
		}
		var msg room.Message
		if err := json.Unmarshal(data, &msg); err != nil {
			s.hub.SendError(code, peer.ID, "invalid JSON message")
			continue
		}
		if err := s.hub.Relay(code, peer.ID, msg); err != nil {
			s.hub.SendError(code, peer.ID, err.Error())
		}
	}
}

// writeLoop drains the peer's outbox. Closing the connection on exit also
// unblocks readLoop when the hub evicts the peer.
func writeLoop(ctx context.Context, conn *websocket.Conn, peer *room.Peer) {
	defer conn.CloseNow()
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-peer.Outbox():
			if !ok {
				return
			}
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := conn.Write(wctx, websocket.MessageText, msg)
			cancel()
			if err != nil {
				return
			}
		case <-ticker.C:
			pctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := conn.Ping(pctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" && s.originAllowed(origin) {
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Add("Vary", "Origin")
			if r.Method == http.MethodOptions {
				h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
				h.Set("Access-Control-Allow-Headers", "Content-Type")
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) originAllowed(origin string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	host := strings.ToLower(u.Host)
	for _, pattern := range s.origins {
		if ok, _ := path.Match(strings.ToLower(pattern), host); ok {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
