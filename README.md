# EasySend Backend

Go signalling server for the EasySend cross-device file transfer portal. Handles pairing rooms and WebSocket message relay.

## Run it

```bash
go run ./cmd/server
```

The server starts on `:8080` by default.

## How it works

The backend coordinates device pairing. It never sees file contents — files go device-to-device over WebRTC, encrypted end-to-end by the browsers.

1. A client creates a room and receives a 6-digit code (e.g. `482-910`).
2. Devices join the room over WebSocket using that code.
3. Peers exchange WebRTC signalling messages (offers, answers, ICE candidates) through the server.
4. Once a direct data channel is established, the server is no longer involved in the transfer.

## API

| Endpoint | Purpose |
| --- | --- |
| `GET /api/health` | Health check |
| `POST /api/rooms` | Create a room and get a 6-digit code |
| `GET /api/rooms/{code}` | Room info: peer count and expiry |
| `GET /ws/rooms/{code}?name=&device=` | Join the room over WebSocket |

### WebSocket messages

All messages are JSON with the shape `{"type", "from", "to", "data"}`.

- On join, the server sends a `welcome` message with your `peerId` and the peers already in the room.
- Other peers receive `peer-joined` when someone joins and `peer-left` when they leave.
- Any other `type` (e.g. `offer`, `answer`, `ice`, `pake`) is relayed. Set `to` to a peer ID for a targeted message, or omit it to broadcast. The server always fills in `from` so peers can't impersonate each other.
- Errors are returned as `error` messages.

### Limits

- Maximum 8 peers per room.
- Empty rooms are deleted after 10 minutes.
- Maximum message size: 64 KB.
- A peer that stops reading messages is disconnected to prevent stalling the room.

## Configuration

| Variable | Default |
| --- | --- |
| `ADDR` | `:8080` |
| `ALLOWED_ORIGINS` | `localhost:5173,127.0.0.1:5173` (CORS and WebSocket origin allow-list) |

## Development

```bash
go test ./...
```
