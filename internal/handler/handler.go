package handler

import (
	"fmt"
	"time"
	"weekend-real-time-chat/internal/hub"

	"github.com/lxzan/gws"
)

type Event interface {
	OnOpen(socket *gws.Conn)                          // connection is established
	OnClose(socket *gws.Conn, err error)              // received a close frame or input/output error occurs
	OnPing(socket *gws.Conn, payload []byte)          // received a ping frame
	OnPong(socket *gws.Conn, payload []byte)          // received a pong frame
	OnMessage(socket *gws.Conn, message *gws.Message) // received a text/binary frame
}

type Handler struct {
	logger   gws.Logger
	eventHub *hub.Hub
}

func NewHandler(logger gws.Logger, evHub *hub.Hub) Event {
	return &Handler{
		logger:   logger,
		eventHub: evHub,
	}
}

// OnClose implements [Event].
func (h *Handler) OnClose(socket *gws.Conn, err error) {
	h.eventHub.Unregister(socket)
}

// OnMessage implements [Event].
func (h *Handler) OnMessage(socket *gws.Conn, message *gws.Message) {

	msg := message.Data.String()

	fmt.Println(msg)

	h.eventHub.Broadcast(message.Bytes())

	socket.WriteString("message received")
}

// OnOpen implements [Event].
func (h *Handler) OnOpen(socket *gws.Conn) {
	_ = socket.SetDeadline(time.Now().Add(5*time.Second + 10*time.Second))

	h.eventHub.Register(socket)

}

// OnPing implements [Event].
func (h *Handler) OnPing(socket *gws.Conn, payload []byte) {
	_ = socket.SetDeadline(time.Now().Add(5*time.Second + 10*time.Second))
	_ = socket.WritePong(nil)
}

// OnPong implements [Event].
func (h *Handler) OnPong(socket *gws.Conn, payload []byte) {
	panic("unimplemented")
}

var _ Event = &Handler{}
