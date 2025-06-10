package model

import (
	"context"
	"errors"
	"go-test-task/internal/domain/valueobject"
	"sync"
	"time"
)

var ErrWaitTimeout = errors.New("wait timeout")

type Waiter struct {
	waitersPerQueue map[string][]chan<- valueobject.Message
	mu              sync.Mutex
}

func NewWaiter() *Waiter {
	return &Waiter{waitersPerQueue: make(map[string][]chan<- valueobject.Message)}
}

func (w *Waiter) WaitMessage(queueName string, waitTimeout time.Duration, ctx context.Context) (valueobject.Message, error) {
	waiterCh := make(chan valueobject.Message)

	w.addWaiterCh(queueName, waiterCh)

	select {
	case msg := <-waiterCh:
		return msg, nil
	case <-ctx.Done():
		w.deleteWaiterCh(queueName, waiterCh)

		return valueobject.Message{}, ErrWaitTimeout
	case <-time.After(waitTimeout):
		w.deleteWaiterCh(queueName, waiterCh)

		return valueobject.Message{}, ErrWaitTimeout
	}
}

func (w *Waiter) Notify(queueName string, message valueobject.Message) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	waiters, isExist := w.waitersPerQueue[queueName]
	if !isExist || len(waiters) == 0 {
		return false
	}

	ch := waiters[0]
	w.waitersPerQueue[queueName] = waiters[1:]

	ch <- message
	close(ch)

	return true
}

func (w *Waiter) addWaiterCh(queueName string, waiterCh chan valueobject.Message) {
	w.mu.Lock()
	w.waitersPerQueue[queueName] = append(w.waitersPerQueue[queueName], waiterCh)
	w.mu.Unlock()
}

func (w *Waiter) deleteWaiterCh(queueName string, toDeleteCh chan<- valueobject.Message) {
	w.mu.Lock()
	defer w.mu.Unlock()

	waiters := w.waitersPerQueue[queueName]
	for i, waiterCh := range waiters {
		if waiterCh == toDeleteCh {
			w.waitersPerQueue[queueName] = append(waiters[:i], waiters[i+1:]...)
			close(waiterCh)

			break
		}
	}

	if len(w.waitersPerQueue[queueName]) == 0 {
		delete(w.waitersPerQueue, queueName)
	}
}
