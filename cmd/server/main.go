package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"go-test-task/internal/controller/queue"
	"go-test-task/internal/domain/model"
	"go-test-task/internal/domain/usecase"
	"go-test-task/internal/infrastructure/memory"
	"go-test-task/internal/transport"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	port := flag.Int("port", 8080, "HTTP port")
	maxQueues := flag.Int("max-queues", 0, "max number of queues")
	maxMessages := flag.Int("max-messages", 0, "max messages in queue")
	defaultWaitTimeout := flag.Int("wait-timeout", 86400, "default timeout (sec)")
	flag.Parse()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	srv := setupServer(*port, *maxQueues, *maxMessages, *defaultWaitTimeout)

	<-sigCh
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Println("server Shutdown error:", err)
	}
}

func setupServer(port, maxQueues, maxMessages, defaultWaitTimeout int) *http.Server {
	addr := fmt.Sprintf(":%d", port)

	srv := &http.Server{
		Addr:    addr,
		Handler: getHttpHandler(maxQueues, maxMessages, defaultWaitTimeout),
	}

	go func() {
		log.Println("Listening on", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server ListenAndServe error: %v", err)
		}
	}()

	return srv
}

func getHttpHandler(maxQueues, maxMessages, defaultWaitTimeout int) http.Handler {
	waiter := model.NewWaiter()
	queueRepo := memory.NewInMemoryQueue(maxQueues, maxMessages)
	brokerRepo := memory.NewInMemoryBroker(maxQueues, func(name string) *model.Queue {
		return model.NewQueue(name, maxMessages, queueRepo)
	})

	broker := model.NewBroker(maxQueues, brokerRepo)
	putter := usecase.NewMessagePutter(broker, waiter)
	getter := usecase.NewMessageGetter(broker, waiter)

	putAction := queue.NewPutAction(putter)
	getAction := queue.NewGetAction(getter, time.Duration(defaultWaitTimeout)*time.Second)

	return transport.NewHttp(putAction, getAction)
}
