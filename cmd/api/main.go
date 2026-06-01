package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"weekend-real-time-chat/internal/handler"
	"weekend-real-time-chat/internal/hub"
	"weekend-real-time-chat/utils"

	"github.com/lxzan/gws"
)

func main() {

	// For later: add a text logger for dev and json logger for tracing
	wrapper := utils.NewSlogWrapper(nil)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
		Addr:    ":8000",
	}

	go func() {
		if err := serv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			wrapper.Error(err.Error())
		}
	}()

	log.Println("Servidor de pe em :8000")

	<-ctx.Done() //lock

	log.Println("encerrando...")

	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	_ = serv.Shutdown(shutdownContext)

	eventHub.Stop()
	log.Println("bye!")
}
