package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"magicbox/backend/internal/httpapi"
	"magicbox/backend/internal/room"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	addr := envOr("ADDR", ":8080")
	origins := splitList(envOr("ALLOWED_ORIGINS", "localhost:5173,127.0.0.1:5173"))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	hub := room.NewHub(10*time.Minute, 8)
	go hub.RunJanitor(ctx, time.Minute)

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.New(hub, origins, log),
		ReadHeaderTimeout: 10 * time.Second,
		// Request contexts derive from ctx so open WebSockets end on shutdown.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error("cannot listen (is another instance already running on this port? set ADDR to change it)", "addr", addr, "err", err)
		os.Exit(1)
	}
	log.Info("magic box backend listening", "addr", ln.Addr().String(), "origins", origins)
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
