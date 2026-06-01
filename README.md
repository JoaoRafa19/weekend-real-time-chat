#  Chat em tempo real (Hub / Broker pattern)

**A ideia.**
 Um servidor WebSocket onde várias conexões trocam mensagens. O problema interessante não é o WebSocket em si — é: como múltiplas goroutines (uma por conexão) escrevem para um conjunto compartilhado de clientes sem corromper o estado e sem usar locks por todo lado?

**A técnica.**
 O `Hub pattern`: uma única goroutine é dona do mapa de conexões. Todo mundo se comunica com ela via channels (register, unregister, broadcast). Isso encarna o lema do Go: "Don't communicate by sharing memory; share memory by communicating." Como só uma goroutine toca o mapa, você não precisa de mutex nele.


**Refinamento técnico.**

Cada conexão tem uma goroutine de leitura (ReadLoop) e o Hub centraliza a escrita/broadcast.
O canal broadcast deve ter buffer, e a escrita para cada cliente precisa de timeout — senão um cliente lento trava o broadcast inteiro (problema do slow consumer).
Use o gws (que você já tem em mãos): ele cuida do handshake e do framing; você só constrói o Hub por cima.

Snippet-guia (o coração do padrão):
```go
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
```

Integrando com o gws (do seu próprio Best Practice):
```go
func (handler *Handler) OnMessage(socket *gws.Conn, message *gws.Message) {
    defer message.Close()
    handler.hub.broadcast <- message.Bytes()
}
func (handler *Handler) OnOpen(socket *gws.Conn)  { handler.hub.register <- socket }
func (handler *Handler) OnClose(socket *gws.Conn, _ error) { handler.hub.unregister <- socket }
```

## Dicas / armadilhas.

O gws recomenda chamar ReadLoop() numa goroutine separada — siga isso, ou o context do request não é coletado pelo GC.
Não acesse h.clients de fora da goroutine Run. Se sentir vontade de pôr um mutex no mapa, é sinal de que vazou o controle do estado.
Desafio extra: troque o broadcast simples pelo gws.NewBroadcaster (comprime a mensagem uma vez só) e adicione "salas" (mapa de room -> clients).