package main

import (
	"encoding/json"
	"net/http"

	"weekend-real-time-chat/internal/handler"
	"weekend-real-time-chat/internal/hub"
	"weekend-real-time-chat/utils"

	"github.com/lxzan/gws"
)

func main() {

	// For later: add a text logger for dev and json logger for tracing
	wrapper := utils.NewSlogWrapper(nil)

	eventHub := hub.NewHub()
	go eventHub.Run() // lida com as operações do EventHub
	eventHandler := handler.NewHandler(wrapper, eventHub)

	upgrader := gws.NewUpgrader(eventHandler, &gws.ServerOption{
		ParallelEnabled:   true,
		Logger:            wrapper,
		Recovery:          gws.Recovery,                         // Exception recovery
		PermessageDeflate: gws.PermessageDeflate{Enabled: true}, // Enable compression
	})

	//http server
	handler := http.NewServeMux()

	handler.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(200)
		if err := json.NewEncoder(w).Encode(map[string]string{"ping": "pong"}); err != nil {
			w.WriteHeader(500)
			w.Write([]byte("error"))
			return
		}
	},
	)

	handler.HandleFunc("/connect", func(w http.ResponseWriter, r *http.Request) {
		socket, err := upgrader.Upgrade(w, r)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte("error"))
			return
		}
		go func() {
			socket.ReadLoop()
		}()

	})

	serv := http.Server{
		Handler: handler,
		Addr: ":8000",
	}

	if err := serv.ListenAndServe(); err != nil {
		wrapper.Error(err.Error())
	}

	
}
