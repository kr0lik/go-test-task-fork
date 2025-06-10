package memory

import (
	"go-test-task/internal/domain/model"
	"sync"
)

type InMemoryBroker struct {
	queuesPerName map[string]*model.Queue
	queueFactory  func(name string) *model.Queue
	mu            sync.Mutex
}

func NewInMemoryBroker(startLen int, queueFactory func(name string) *model.Queue) *InMemoryBroker {
	return &InMemoryBroker{
		queuesPerName: make(map[string]*model.Queue, startLen),
		queueFactory:  queueFactory,
	}
}

func (r *InMemoryBroker) CreateQueue(queueName string) (*model.Queue, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.queuesPerName[queueName]; exists {
		return r.queuesPerName[queueName], nil
	}

	queue := r.queueFactory(queueName)
	r.queuesPerName[queueName] = queue

	return queue, nil
}

func (r *InMemoryBroker) GetQueue(queueName string) (*model.Queue, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	queue, isExist := r.queuesPerName[queueName]
	if !isExist {
		return nil, model.ErrQueueNotFound
	}

	return queue, nil
}

func (r *InMemoryBroker) CountQueues() (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.queuesPerName), nil
}
