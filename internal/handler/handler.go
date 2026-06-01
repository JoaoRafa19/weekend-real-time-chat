package handler

import (
	"bytes"
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
	//devolve o buffer para a pool para ser reaproveitado
	defer message.Close()
	// clona o em outro buffer para que o hub nao bloqueie o envio de novas mensagens
	// enquanto esta lendo o buffer dessa mensagem
	payload := bytes.Clone(message.Bytes())

	h.eventHub.Broadcast(payload)

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

}

var _ Event = &Handler{}
