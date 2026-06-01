package hub

import "github.com/lxzan/gws"

type Hub struct {
	clients    map[*gws.Conn]struct{}
	broadcast  chan []byte
	register   chan *gws.Conn
	unregister chan *gws.Conn
}

func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.clients[c] = struct{}{}
		case c := <-h.unregister:
			delete(h.clients, c)
		case msg := <-h.broadcast:
			for c := range h.clients {
				_ = c.WriteMessage(gws.OpcodeText, msg) // idealmente com timeout
			}
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

func NewHub() *Hub {
	return &Hub{
		broadcast:  make(chan []byte),
		register:   make(chan *gws.Conn),
		unregister: make(chan *gws.Conn),
		clients:    make(map[*gws.Conn]struct{}),
	}
}
