package hub

import (
	"log/slog"

	"github.com/lxzan/gws"
)

type Client struct {
	conn *gws.Conn
	send chan []byte
}

func NewClient(conn *gws.Conn) *Client {
	return &Client{
		conn: conn,
		send: make(chan []byte, 256),
	}
}

func (c *Client) writePump() {
	for msg := range c.send {
		_ = c.conn.WriteMessage(gws.OpcodeText, msg)
	}
}

type Hub struct {
	clients    map[*gws.Conn]*Client
	broadcast  chan []byte
	register   chan *gws.Conn
	unregister chan *gws.Conn
}

func NewHub() *Hub {
	return &Hub{
		broadcast:  make(chan []byte),
		register:   make(chan *gws.Conn),
		unregister: make(chan *gws.Conn),
		clients:    make(map[*gws.Conn]*Client),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case conn := <-h.register:
			client := NewClient(conn)
			h.clients[conn] = client
			go client.writePump()

		case c := <-h.unregister:
			if client, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(client.send)
			}
		case message := <-h.broadcast:
			for conn, client := range h.clients {
				select {
				case client.send <- message:
				default: //backpressure isolation
					// buffer cheio = cliente não acompanha → expulsa
					slog.Info("Consumer kicked!")
					delete(h.clients, conn)
					close(client.send)
					client.conn.WriteClose(1000, nil)
				}
			}

			/* Slow consumer com WriteMessage Bloqueante
			case msg := <-h.broadcast:
				for c := range h.clients {
					_ = c.WriteMessage(gws.OpcodeText, msg) // idealmente com timeout
				}
			*/
		}
	}
}

func (hub *Hub) Unregister(conn *gws.Conn) {
	hub.unregister <- conn
}

func (hub *Hub) Register(conn *gws.Conn) {
	hub.register <- conn
}

func (hub *Hub) Broadcast(message []byte) {
	hub.broadcast <- message
}
